#!/usr/bin/env python3
"""The independent implementation behind the frozen sealed-blob vector.

sealed/blobfile_test.go:TestBlobFrozenVectorMatchesAnIndependentImplementation
pins the values this script prints. It shares no code with the Go codec: the
key derivation and the AEAD are the `cryptography` package's (OpenSSL), and the
file layout is written here from docs/specs/at-rest-encryption-spec.md §4.2.1
and §2.4/§2.5 alone:

    file    = header(24) ‖ chunk_0 ‖ … ‖ chunk_{n-1}
    header  = 00 'H' 'D' 'B' ‖ version 02 ‖ purpose 06 ‖ flags 00 ‖ reserved 00
              ‖ BE64(epoch id) ‖ BE64(gen = 0)
    chunk_i = nonce_i(12) ‖ AES-256-GCM(file key, nonce_i, plaintext_i, AAD_i)
              (ciphertext, then the 16-byte tag)
    AAD_i   = header ‖ BE64(i) ‖ BE64(n)          i counted from 0
    n       = ceil(size / 65536); every chunk but the last is 65536 bytes
    file key = HKDF-SHA256(epoch key, salt none, info "hoardDB/v2/06/" + name)

The inputs are deterministic patterns, stated below, so the Go test rebuilds
them without reading a file.

Run (from hoardDB/sealed/testdata; needs `pip install cryptography`):

    python3 blob-seal-vector.py > blob-seal-vector.txt

The output is frozen. If the Go test stops matching it, the format moved:
regenerating the expected values from the Go code defeats the test.
"""

import hashlib
import struct

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

CHUNK_SIZE = 65536
EPOCH_KEY = bytes(range(1, 33))  # 01 02 … 20
EPOCH_ID = 258  # 0x0102: both bytes differ, so the byte order is visible
PAYLOAD_NAME = "2a7-0123456789abcdef-01890a5d-ac96-774b-bcce-b302099a8057"
SIZES = (65536, 65537, 131072)  # one full chunk; a one-byte last chunk; an exact multiple


def plaintext(size):
    """Byte j is j mod 251."""
    return bytes(j % 251 for j in range(size))


def nonce(chunk_index):
    """The nonce stream's byte k is (0xA0 + k) mod 256; chunk i takes bytes 12i … 12i+11."""
    return bytes((0xA0 + k) % 256 for k in range(12 * chunk_index, 12 * chunk_index + 12))


def seal(size):
    info = ("hoardDB/v2/06/" + PAYLOAD_NAME).encode()
    file_key = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=info).derive(EPOCH_KEY)
    header = b"\x00HDB" + bytes([0x02, 0x06, 0x00, 0x00]) + struct.pack(">Q", EPOCH_ID) + struct.pack(">Q", 0)
    content = plaintext(size)
    chunk_count = (size + CHUNK_SIZE - 1) // CHUNK_SIZE
    sealed_chunks = []
    for index in range(chunk_count):
        aad = header + struct.pack(">Q", index) + struct.pack(">Q", chunk_count)
        chunk = content[index * CHUNK_SIZE:(index + 1) * CHUNK_SIZE]
        sealed_chunks.append(nonce(index) + AESGCM(file_key).encrypt(nonce(index), chunk, aad))
    return header, sealed_chunks


def main():
    for size in SIZES:
        header, sealed_chunks = seal(size)
        sealed_file = header + b"".join(sealed_chunks)
        first, last = sealed_chunks[0], sealed_chunks[-1]
        print(f"size {size}")
        print(f"  file length      {len(sealed_file)}")
        print(f"  chunks           {len(sealed_chunks)}")
        print(f"  file sha256      {hashlib.sha256(sealed_file).hexdigest()}")
        print(f"  header           {header.hex()}")
        print(f"  first nonce      {first[:12].hex()}")
        print(f"  first tag        {first[-16:].hex()}")
        print(f"  last nonce       {last[:12].hex()}")
        print(f"  last tag         {last[-16:].hex()}")


if __name__ == "__main__":
    main()
