import asyncio
import subprocess
from pathlib import Path
from typing import Optional

def ffmpeg_cut(input_bytes: bytes, offset: int) -> Optional[bytes]:
    """
    Обрезать аудиофайл (mp3) с помощью ffmpeg с заданного offset (секунды).
    Возвращает байты нового файла или None при ошибке.
    """
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as in_file:
        in_file.write(input_bytes)
        in_file.flush()
        in_path = in_file.name
    out_path = in_path + '.cut.mp3'
    try:
        cmd = [
            'ffmpeg', '-y', '-ss', str(offset), '-i', in_path,
            '-vn', '-acodec', 'copy', out_path
        ]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            return None
        with open(out_path, 'rb') as out_file:
            return out_file.read()
    finally:
        Path(in_path).unlink(missing_ok=True)
        Path(out_path).unlink(missing_ok=True)
