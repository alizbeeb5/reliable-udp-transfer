# Member 1: Ghadi Zouki
# (AI informed me about the data class and other related functions)

import struct
from dataclasses import dataclass

DATA = 1
ACK = 2
END = 3
CHUNK_SIZE = 1024
MAX_WINDOW = 4096
HEADER = struct.Struct("!BIH")
_MAX_SEQ = 2**32 - 1


@dataclass(frozen=True)
class Packet:
    packet_type: int
    seq: int
    payload: bytes


def _validate_fields(packet_type: int, seq: int, payload_len: int) -> None:
    if not isinstance(packet_type, int) or packet_type not in (DATA, ACK, END):
        raise ValueError("Unknown packet type.")
    if not isinstance(seq, int) or not 0 <= seq <= _MAX_SEQ:
        raise ValueError("Sequence number is out of range.")
    if packet_type in (DATA, END) and seq == _MAX_SEQ:
        raise ValueError("No sequence number is available to acknowledge this packet.")
    if packet_type == DATA and not 1 <= payload_len <= CHUNK_SIZE:
        raise ValueError("DATA must contain 1 to 1024 bytes.")
    if packet_type == ACK and payload_len != 0:
        raise ValueError("ACK must have an empty payload.")
    if packet_type == END and payload_len != 32:
        raise ValueError("END must contain a 32-byte SHA-256 digest.")


def encode_packet(packet_type: int, seq: int, payload: bytes = b"") -> bytes:
    """Build a datagram from a packet type, sequence number, and payload."""
    _validate_fields(packet_type, seq, len(payload))
    return HEADER.pack(packet_type, seq, len(payload)) + payload


def decode_packet(datagram: bytes) -> Packet:
    """Read a datagram, raising ValueError if it violates the format."""
    if len(datagram) < HEADER.size:
        raise ValueError("Packet is missing part or all of its header.")

    packet_type, seq, payload_len = HEADER.unpack_from(datagram)
    payload = datagram[HEADER.size:]

    if len(payload) != payload_len:
        raise ValueError("Payload length does not match the header.")

    _validate_fields(packet_type, seq, payload_len)
    return Packet(packet_type, seq, payload)