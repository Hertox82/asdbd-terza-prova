#!/usr/bin/env python3
"""Scarica e verifica i tre dataset NYC TLC di luglio 2025."""

import hashlib
import json
import sys
from http.client import HTTPException
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlopen


RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
CHUNK_SIZE = 1024 * 1024


def verify(size, digest, expected_size, expected_sha256):
    if size != expected_size:
        raise ValueError(f"dimensione attesa {expected_size} byte, ricevuti {size}")
    if digest.hexdigest() != expected_sha256:
        raise ValueError("SHA-256 diverso da quello riportato nel manifest")


def verify_existing(path, expected_size, expected_sha256):
    with path.open("rb") as source:
        size = path.stat().st_size
        if size != expected_size:
            raise ValueError(f"dimensione attesa {expected_size} byte, trovati {size}")
        digest = hashlib.file_digest(source, "sha256")
    verify(size, digest, expected_size, expected_sha256)


def download(path, expected_size, expected_sha256):
    temporary_path = None
    try:
        with NamedTemporaryFile(dir=path.parent, suffix=".part", delete=False) as output:
            temporary_path = Path(output.name)
            digest = hashlib.sha256()
            size = 0
            with urlopen(BASE_URL + path.name, timeout=60) as response:
                while chunk := response.read(CHUNK_SIZE):
                    size += len(chunk)
                    if size > expected_size:
                        raise ValueError(f"download oltre i {expected_size} byte attesi")
                    output.write(chunk)
                    digest.update(chunk)
            verify(size, digest, expected_size, expected_sha256)

        # Pubblica il file completo senza sostituire un originale comparso nel frattempo.
        try:
            path.hardlink_to(temporary_path)
        except FileExistsError:
            verify_existing(path, expected_size, expected_sha256)
    finally:
        if temporary_path is not None:
            temporary_path.unlink()


def main():
    try:
        manifest = json.loads((RAW_DIR / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        print(f"Impossibile leggere il manifest: {error}", file=sys.stderr)
        return 1

    failed = False
    for name, metadata in manifest.items():
        path = RAW_DIR / name
        expected_size = metadata["size_bytes"]
        expected_sha256 = metadata["sha256"]
        try:
            if path.exists():
                verify_existing(path, expected_size, expected_sha256)
                print(f"{name}: già presente, verificato")
            else:
                print(f"{name}: scaricamento in corso", flush=True)
                download(path, expected_size, expected_sha256)
                print(f"{name}: disponibile e verificato")
        except (OSError, ValueError, HTTPException) as error:
            print(f"{name}: {error}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
