"""Google Gemini 3.5 Live Speech-to-Text Transcription Service.

Streams raw 16-bit PCM at 16kHz (mono, little-endian) in 100ms chunks (3,200 bytes)
over Gemini Live bidirectional WebSockets using the gemini-3.5-transcribe-live model.
Configured with SMART transcription mode for disfluency removal and structured formatting.
"""

import asyncio
import base64
import json
import logging
import os
import subprocess
import tempfile
from typing import Optional

import httpx
import websockets

from app.services.db import telephony_db
from app.services.storage_r2 import storage_r2

logger = logging.getLogger("wasid.services.transcribe_gemini")

DEFAULT_API_KEY = ""
GEMINI_LIVE_MODEL = "models/gemini-3.5-transcribe-live"
CHUNK_SIZE_BYTES = 3200  # 100ms of 16kHz 16-bit mono PCM (16000 * 0.1 * 2)


class GeminiTranscriptionService:
    """Production service for live and recording transcriptions via Gemini 3.5 Transcribe Live."""

    def __init__(self):
        self.model = os.getenv("GEMINI_TRANSCRIBE_MODEL", GEMINI_LIVE_MODEL)

    def get_api_key(self) -> str:
        """Fetch the Gemini API key from environment."""
        return os.getenv("GEMINI_API_KEY", DEFAULT_API_KEY)

    def is_configured(self) -> bool:
        """Check if Gemini transcription is ready to operate."""
        return bool(self.get_api_key())

    async def _pcm_from_audio_bytes(self, audio_bytes: bytes) -> bytes:
        """Convert arbitrary audio bytes (M4A, MP3, WAV) to raw 16kHz 16-bit mono PCM."""
        def _ffmpeg_convert() -> bytes:
            with tempfile.NamedTemporaryFile(suffix=".audio", delete=False) as tf_in:
                tf_in.write(audio_bytes)
                in_path = tf_in.name

            try:
                cmd = [
                    "ffmpeg", "-y", "-i", in_path,
                    "-f", "s16le",
                    "-acodec", "pcm_s16le",
                    "-ar", "16000",
                    "-ac", "1",
                    "-"
                ]
                return subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
            except Exception as e:
                logger.error("FFmpeg conversion error: %s", e)
                return b""
            finally:
                if os.path.exists(in_path):
                    try:
                        os.unlink(in_path)
                    except OSError:
                        pass

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _ffmpeg_convert)

    async def transcribe_audio_bytes(
        self,
        audio_bytes: bytes,
        mode: str = "SMART",
    ) -> Optional[str]:
        """Transcribe audio bytes using Gemini 3.5 Transcribe Live streaming pipeline."""
        api_key = self.get_api_key()
        if not api_key:
            logger.error("Cannot transcribe: GEMINI_API_KEY is not configured")
            return None

        if not audio_bytes or len(audio_bytes) < 100:
            logger.warning("Empty or truncated audio bytes passed to transcribe_audio_bytes")
            return None

        try:
            raw_pcm = await self._pcm_from_audio_bytes(audio_bytes)
            if not raw_pcm:
                logger.warning("FFmpeg generated empty PCM audio stream, attempting REST fallback")
                return await self._transcribe_via_rest_fallback(audio_bytes)

            ws_url = (
                f"wss://generativelanguage.googleapis.com/ws/"
                f"google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={api_key}"
            )

            logger.info(
                "Initiating Gemini Live transcription (%d PCM bytes, model: %s, mode: %s)",
                len(raw_pcm),
                self.model,
                mode,
            )

            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                # 1. Send Setup Message
                setup_msg = {
                    "setup": {
                        "model": self.model,
                        "generationConfig": {
                            "responseModalities": ["TEXT"]
                        },
                        "inputAudioTranscription": {
                            "languageCodes": [],
                            "mode": mode.upper() if mode else "SMART"
                        }
                    }
                }
                await ws.send(json.dumps(setup_msg))

                # Wait for setupComplete response
                init_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                logger.debug("Gemini Live connection initialized: %s", str(init_resp)[:100])

                transcripts = []
                stop_receiving = asyncio.Event()

                async def _receive_loop():
                    while not stop_receiving.is_set():
                        try:
                            raw_msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            data = json.loads(raw_msg)
                            server_content = data.get("serverContent") or {}
                            input_tx = server_content.get("inputTranscription") or {}
                            if input_tx and "text" in input_tx:
                                text_segment = input_tx["text"].strip()
                                if text_segment and (not transcripts or transcripts[-1] != text_segment):
                                    transcripts.append(text_segment)
                        except asyncio.TimeoutError:
                            continue
                        except Exception as ex:
                            logger.debug("Receive loop finished: %s", ex)
                            break

                receiver_task = asyncio.create_task(_receive_loop())

                # 2. Stream 100ms PCM chunks
                total_len = len(raw_pcm)
                for offset in range(0, total_len, CHUNK_SIZE_BYTES):
                    chunk = raw_pcm[offset:offset + CHUNK_SIZE_BYTES]
                    audio_payload = {
                        "realtimeInput": {
                            "audio": {
                                "data": base64.b64encode(chunk).decode("utf-8"),
                                "mimeType": "audio/pcm;rate=16000"
                            }
                        }
                    }
                    await ws.send(json.dumps(audio_payload))
                    await asyncio.sleep(0.01)  # High-throughput streaming

                # 3. Signal Audio Stream End
                await ws.send(json.dumps({"realtimeInput": {"audioStreamEnd": True}}))

                # Allow final transcription packets to arrive
                await asyncio.sleep(4.0)
                stop_receiving.set()
                await receiver_task

                full_text = " ".join(transcripts).strip()
                if full_text:
                    logger.info("Gemini Live transcription completed successfully (%d chars)", len(full_text))
                    return full_text

        except Exception as e:
            logger.warning("Gemini Live WebSocket transcription encountered error: %s. Falling back to REST API.", e)

        # Fallback to direct Gemini multimodal audio transcription via REST
        return await self._transcribe_via_rest_fallback(audio_bytes)

    async def _transcribe_via_rest_fallback(self, audio_bytes: bytes) -> Optional[str]:
        """Resilient fallback transcribing audio bytes using Gemini multimodal REST API."""
        api_key = self.get_api_key()
        if not api_key:
            logger.error("REST fallback: GEMINI_API_KEY not set")
            return None

        # Auto-detect MIME type from file header
        mime_type = "audio/mpeg"  # default
        if audio_bytes[:4] == b'\x00\x00\x00\x20' or audio_bytes[4:8] == b'ftyp':
            mime_type = "audio/mp4"
        elif audio_bytes[:3] == b'ID3' or (audio_bytes[0:2] == b'\xff\xfb'):
            mime_type = "audio/mpeg"
        elif audio_bytes[:4] == b'RIFF':
            mime_type = "audio/wav"
        elif audio_bytes[:4] == b'OggS':
            mime_type = "audio/ogg"

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
        b64_audio = base64.b64encode(audio_bytes).decode("utf-8")

        logger.info("REST fallback: sending %d bytes as %s to Gemini", len(audio_bytes), mime_type)

        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": (
                                "Transcribe the following call audio accurately. "
                                "Clean up disfluencies, remove filler words, format numbers and punctuation naturally. "
                                "Output ONLY the cleaned transcription text with no preamble or commentary."
                            )
                        },
                        {
                            "inlineData": {
                                "mimeType": mime_type,
                                "data": b64_audio
                            }
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.2
            }
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                res = await client.post(url, json=payload)
                if res.status_code == 200:
                    data = res.json()
                    candidates = data.get("candidates") or []
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        if parts and "text" in parts[0]:
                            text_out = parts[0]["text"].strip()
                            logger.info("Gemini REST fallback transcription succeeded (%d chars)", len(text_out))
                            return text_out
                    logger.warning("Gemini REST fallback returned no candidates: %s", str(data)[:200])
                else:
                    logger.warning("Gemini REST fallback failed with status %d: %s", res.status_code, res.text[:300])
        except Exception as ex:
            logger.error("Gemini REST fallback error: %s", ex)

        return None

    async def transcribe_recording(
        self,
        recording_id: str,
        force: bool = False,
    ) -> Optional[str]:
        """Fetch recording audio from R2, transcribe using Gemini 3.5 Live STT, and persist to database."""
        try:
            rec = await telephony_db.get_recording(recording_id)
            if not rec:
                logger.warning("Recording %s not found in database", recording_id)
                return None

            if rec.get("transcription") and not force:
                logger.debug("Recording %s already has transcription", recording_id)
                return rec.get("transcription")

            # Mark status as transcribing
            await telephony_db.update_recording_transcription(recording_id, "", status="transcribing")

            # Get audio bytes from R2
            object_key = rec.get("storage_object_key")
            audio_bytes = None
            if object_key and storage_r2.is_configured():
                try:
                    if await storage_r2.object_exists(object_key):
                        audio_bytes = await storage_r2.get_object_bytes(object_key)
                    elif not object_key.endswith(".mp3"):
                        mp3_key = f"{object_key}.mp3"
                        if await storage_r2.object_exists(mp3_key):
                            audio_bytes = await storage_r2.get_object_bytes(mp3_key)
                except Exception as r2_err:
                    logger.error("R2 fetch error for recording %s: %s", recording_id, r2_err)

            if not audio_bytes:
                logger.error("No audio bytes available in R2 for recording %s (key: %s)", recording_id, object_key)
                await telephony_db.update_recording_transcription(recording_id, "", status="failed")
                return None

            logger.info("Fetched %d bytes from R2 for recording %s, starting transcription...", len(audio_bytes), recording_id)

            # Try REST fallback first (more reliable than WebSocket for recorded audio)
            transcript = await self._transcribe_via_rest_fallback(audio_bytes)

            # If REST failed, try WebSocket streaming
            if not transcript:
                logger.info("REST fallback returned no result for %s, trying WebSocket streaming...", recording_id)
                transcript = await self.transcribe_audio_bytes(audio_bytes, mode="SMART")

            if transcript:
                await telephony_db.update_recording_transcription(recording_id, transcript, status="completed")
                logger.info("Saved transcription for recording %s (%d chars)", recording_id, len(transcript))
                return transcript
            else:
                await telephony_db.update_recording_transcription(recording_id, "", status="failed")
                logger.warning("Transcription generation failed for recording %s", recording_id)
                return None

        except Exception as e:
            logger.exception("Unhandled error in transcribe_recording for %s: %s", recording_id, e)
            try:
                await telephony_db.update_recording_transcription(recording_id, "", status="failed")
            except Exception:
                logger.exception("Failed to update status to 'failed' for recording %s", recording_id)
            return None


# Global singleton instance
gemini_transcribe = GeminiTranscriptionService()
