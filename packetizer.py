import hashlib
from dataclasses import dataclass
from pathlib import Path

from protocol import CHUNK_SIZE, DATA, END, encode_packet

@dataclass(frozen=True)
class PreparedTransfer:
    file_name: str
    file_size: int
    sha256_digest: bytes
    packets: tuple[bytes, ...]

def prepare_transfer(path: str) -> PreparedTransfer:
    file_path = Path(path)
    packet_list = []
    total_bytes = 0
    hasher = hashlib.sha256()

    with open(file_path, "rb") as file:
        while True:
            chunk = file.read(CHUNK_SIZE)
            if not chunk:
                break

            packet_number = len(packet_list)
            packet = encode_packet(DATA, packet_number, chunk)
            packet_list.append(packet)

            hasher.update(chunk)
            total_bytes += len(chunk)

    file_hash = hasher.digest()
    end_packet = encode_packet(END, len(packet_list), file_hash)
    packet_list.append(end_packet)

    return PreparedTransfer(
        file_name=file_path.name,
        file_size=total_bytes,
        sha256_digest=file_hash,
        packets=tuple(packet_list),
    )