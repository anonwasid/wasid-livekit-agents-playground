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
            return "[No speech detected in call recording]"

        try:
            raw_pcm = await self._pcm_from_audio_bytes(audio_bytes)
            if not raw_pcm:
                logger.warning("FFmpeg generated empty PCM audio stream")
                return "[No speech detected in call recording]"

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
                init_resp = await asyncio.wait_for(ws.recv(), timeout=15.0)
                logger.debug("Gemini Live connection initialized: %s", str(init_resp)[:100])

                transcripts = []
                streaming_done = asyncio.Event()

                async def receive_loop():
                    while True:
                        try:
                            # While streaming use 0.5s timeout, after stream end wait up to 6.0s for next packet
                            timeout_val = 6.0 if streaming_done.is_set() else 0.5
                            raw_msg = await asyncio.wait_for(ws.recv(), timeout=timeout_val)
                            data = json.loads(raw_msg)
                            server_content = data.get("serverContent") or {}
                            input_tx = server_content.get("inputTranscription") or {}
                            if input_tx and "text" in input_tx:
                                text_segment = input_tx["text"].strip()
                                if text_segment and (not transcripts or transcripts[-1] != text_segment):
                                    transcripts.append(text_segment)
                        except asyncio.TimeoutError:
                            if streaming_done.is_set():
                                break
                        except Exception as ex:
                            logger.debug("Receive loop finished: %s", ex)
                            break

                recv_task = asyncio.create_task(receive_loop())

                # 2. Stream 100ms PCM chunks (3,200 bytes per chunk at 16kHz mono)
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
                    await asyncio.sleep(0.01)

                # 3. Signal Audio Stream End
                await ws.send(json.dumps({"realtimeInput": {"audioStreamEnd": True}}))
                streaming_done.set()
                logger.info("AudioStreamEnd sent, waiting for remaining transcripts...")

                # Wait for receive loop to capture all final transcription segments
                await recv_task

                full_text = " ".join(transcripts).strip()
                if full_text:
                    logger.info("Gemini Live transcription completed successfully (%d chars, %d segments)", len(full_text), len(transcripts))
                    return full_text
                else:
                    logger.info("Gemini Live returned 0 segments for audio (likely silence/no speech)")
                    return "[No speech detected in call recording]"

        except Exception as e:
            logger.exception("Gemini Live WebSocket transcription encountered error: %s", e)
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

            logger.info("Fetched %d bytes from R2 for recording %s, starting Gemini 3.5 Live STT...", len(audio_bytes), recording_id)

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
                pass
            return None


# Global singleton instance
gemini_transcribe = GeminiTranscriptionService()
