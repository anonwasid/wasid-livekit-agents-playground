"""Audio Transcoding Service for Telephony Call Recordings.

Transcodes incoming .m4a (AAC) recordings into standard .mp3 (MPEG-1 Audio Layer 3)
at 128 kbps stereo / 44.1 kHz, preserving both sides of the voice conversation and
caching the result in Cloudflare R2.
"""

import asyncio
import logging
import shutil
from typing import Optional

from app.services.storage_r2 import storage_r2

logger = logging.getLogger(__name__)


import tempfile
import os

async def transcode_to_mp3(audio_bytes: bytes) -> bytes:
    """Transcode raw audio bytes (e.g. M4A / AAC) to MP3 using ffmpeg via temp files for seekability."""
    ffmpeg_bin = shutil.which("ffmpeg") or "ffmpeg"

    # M4A / MP4 demuxing requires seekable file handles
    loop = asyncio.get_running_loop()

    def _sync_transcode() -> bytes:
        with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as in_f, \
             tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as out_f:
            in_path = in_f.name
            out_path = out_f.name
            in_f.write(audio_bytes)
            in_f.flush()

        try:
            cmd = [
                ffmpeg_bin,
                "-y",
                "-i", in_path,
                "-codec:a", "libmp3lame",
                "-b:a", "128k",
                "-ar", "44100",
                "-ac", "2",
                out_path,
            ]
            import subprocess
            proc = subprocess.run(cmd, capture_output=True)
            if proc.returncode != 0:
                err = proc.stderr.decode("utf-8", errors="replace")
                logger.error("FFmpeg transcode failed (code %d): %s", proc.returncode, err)
                raise RuntimeError(f"FFmpeg transcode failed: {err[:200]}")

            with open(out_path, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(in_path):
                try:
                    os.remove(in_path)
                except Exception:
                    pass
            if os.path.exists(out_path):
                try:
                    os.remove(out_path)
                except Exception:
                    pass

    return await loop.run_in_executor(None, _sync_transcode)


async def ensure_mp3_in_r2(object_key: str) -> Optional[str]:
    """Ensure that an MP3 version of the recording exists in Cloudflare R2.
    
    If the object is already an MP3, returns object_key.
    Otherwise checks if the corresponding .mp3 exists. If missing, downloads the
    .m4a file, transcodes it to .mp3 via FFmpeg, uploads it to R2, and returns the mp3 key.
    """
    if not object_key:
        return None

    if object_key.endswith(".mp3"):
        return object_key

    # Expected MP3 key
    mp3_key = object_key.rsplit(".", 1)[0] + ".mp3"

    try:
        # Check if already cached in R2
        if await storage_r2.object_exists(mp3_key):
            return mp3_key

        # Download original recording from R2
        logger.info("Downloading %s for MP3 transcoding...", object_key)
        raw_bytes = await storage_r2.get_object_bytes(object_key)
        if not raw_bytes:
            logger.warning("Could not download original object %s from R2", object_key)
            return None

        # Transcode to MP3
        logger.info("Transcoding %d bytes of %s to MP3...", len(raw_bytes), object_key)
        mp3_bytes = await transcode_to_mp3(raw_bytes)

        # Upload MP3 back to R2
        uploaded = await storage_r2.put_object_bytes(
            mp3_key,
            mp3_bytes,
            content_type="audio/mpeg",
        )
        if uploaded:
            logger.info("Successfully cached MP3 in R2: %s (%d bytes)", mp3_key, len(mp3_bytes))
            return mp3_key
        else:
            logger.warning("Failed to upload transcoded MP3 %s to R2", mp3_key)
            return None

    except Exception as e:
        logger.error("Error ensuring MP3 in R2 for %s: %s", object_key, e)
        return None
