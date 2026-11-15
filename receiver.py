# Member 3: Yara El Rohayem
# Part 3: receiver
# AI assistance: Claude was used to help with library imports and syntax.
# OpenAI assistance for CLI wiring, cleanup, and integration tests.

import argparse
import hashlib
import socket

from protocol import ACK, DATA, END, MAX_WINDOW, decode_packet, encode_packet

HOST = "127.0.0.1"
PORT = 9000
OUTPUT_FILE = "received_file"


def finish_file(file_data, end_hash, output_path=OUTPUT_FILE):
    with open(output_path, "wb") as f:
        f.writelines(file_data)

    hasher = hashlib.sha256()
    with open(output_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)

    if hasher.digest() == end_hash:
        return "MATCH"
    return "MISMATCH"


def receive_file(sock, output_path):
    expected = 0
    buffer = {}
    file_data = []
    peer = None
    done = False

    stats = {"received": 0, "in_order": 0, "out_of_order": 0, "duplicates": 0, "acks_sent": 0}
    result = "PENDING"

    while True:
        datagram, addr = sock.recvfrom(65535)

        try:
            packet = decode_packet(datagram)
        except ValueError:
            print("Malformed packet, ignored")
            continue

        if packet.packet_type not in (DATA, END):
            continue

        if peer is None:
            peer = addr
        elif addr != peer:
            continue

        seq = packet.seq
        stats["received"] += 1

        if done:
            if seq < expected:
                sock.sendto(encode_packet(ACK, expected), peer)
                stats["acks_sent"] += 1
                print(f"RECV {seq} after finish, duplicate → ACK {expected}")
            continue

        if seq == expected:
            stats["in_order"] += 1
            if packet.packet_type == DATA:
                file_data.append(packet.payload)
            else:
                result = finish_file(file_data, packet.payload, output_path)
                done = True
            expected += 1

            while not done and expected in buffer:
                saved = buffer.pop(expected)
                if saved.packet_type == DATA:
                    file_data.append(saved.payload)
                else:
                    result = finish_file(file_data, saved.payload, output_path)
                    done = True
                expected += 1

            if expected > seq + 1:
                print(f"RECV {seq} fills gap → ACK {expected}")
            else:
                print(f"RECV {seq} in order → ACK {expected}")

        elif seq < expected or seq in buffer:
            stats["duplicates"] += 1
            print(f"RECV {seq} duplicate, discarded → ACK {expected}")

        elif seq < expected + MAX_WINDOW:
            buffer[seq] = packet
            stats["out_of_order"] += 1
            print(f"RECV {seq} out of order, buffered → DUP ACK {expected}")

        else:
            print(f"RECV {seq} too far ahead, discarded → ACK {expected}")

        sock.sendto(encode_packet(ACK, expected), peer)
        stats["acks_sent"] += 1

        if done:
            print("----- RECEIVER SUMMARY -----")
            print(f"Packets received: {stats['received']}")
            print(f"In order: {stats['in_order']}, out of order: {stats['out_of_order']}, duplicates: {stats['duplicates']}")
            print(f"ACKs sent: {stats['acks_sent']}")
            print(f"Bytes written: {sum(len(piece) for piece in file_data)}")
            print(f"File check: {result}")
            buffer.clear()
            file_data.clear()


def main():
    parser = argparse.ArgumentParser(description="Receive one reliable UDP file transfer.")
    parser.add_argument("--host", default=HOST, help="local IPv4 address to bind")
    parser.add_argument("--port", type=int, default=PORT, help="local UDP port")
    parser.add_argument("--output", default=OUTPUT_FILE, help="where to save the received file")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((args.host, args.port))
            print(f"Receiver listening on {args.host}:{args.port}, output: {args.output}", flush=True)
            receive_file(sock, args.output)
    except KeyboardInterrupt:
        print("\nReceiver stopped.")
        return 130
    except OSError as error:
        parser.exit(1, f"Receiver error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


