# EECE 350 Group 9: sender entry point integrating Ghadi Zouki's packetizer
# and channel with Ali Hadi Zbeeb's sender window. Wiring drafted with OpenAI assistance.

import argparse
import select
import socket
import sys
import time

from channel import UnreliableChannel
from packetizer import prepare_transfer
from protocol import ACK, MAX_WINDOW, decode_packet
from window import SenderWindow

POLL_INTERVAL = 0.01
ACK_BATCH = 64
EVENT_COLORS = {
    "ACK": 32, "DROP": 31, "TIMEOUT": 31,
    "DUP_ACK": 33, "RETRANSMIT": 33, "REORDERED": 35, "WINDOW": 36,
}


def show_event(event):
    name = event["event"]
    seq = event.get("seq")
    if name == "WINDOW":
        slots = " ".join(f"{packet['seq']}:{packet['state']}" for packet in event["packets"])
        message = f"WINDOW base={event['base']} next={event['next_seq']}  {slots or '(empty)'}"
    elif name == "ACK":
        message = f"ACK {event['ack_next']} -> ACKNOWLEDGED {event['acked']}"
    elif name == "DUP_ACK":
        message = f"DUP ACK {event['ack_next']} ({event['dup_ack_count']})"
    elif name == "RETRANSMIT":
        message = f"RETRANSMIT {seq} ({event['reason']}) -> RETRANSMITTED, attempt {event['attempt']}"
    elif name == "TIMEOUT":
        message = f"TIMEOUT {seq} (RTO {event['rto']:.3f}s)"
    elif name == "REORDERED":
        message = f"REORDERED {seq} after {event['after_seq']}"
    elif name == "SEND" and event["side"] == "sender":
        message = f"SEND {seq} -> SENT"
    elif name in ("DROP", "HOLD", "FLUSH"):
        message = f"{name} {seq}"
    elif name in ("STALE_ACK", "ACK_UNSENT"):
        message = f"IGNORE ACK {event['ack_next']} ({event['reason']})"
    else:
        return
    color = EVENT_COLORS.get(name)
    if color and sys.stdout.isatty():
        message = f"\033[{color}m{message}\033[0m"
    print(message)


def read_acks(sock, peer, window):
    # Wait once, then drain only ready datagrams. Bound the batch so a stream
    # of irrelevant packets cannot indefinitely delay the channel and timer.
    wait = POLL_INTERVAL
    for _ in range(ACK_BATCH):
        if window.done() or not select.select([sock], [], [], wait)[0]:
            return
        datagram, address = sock.recvfrom(65535)
        wait = 0
        if address != peer:
            continue
        try:
            packet = decode_packet(datagram)
        except ValueError:
            print("Ignored malformed datagram from receiver")
            continue
        if packet.packet_type == ACK:
            window.on_ack(packet.seq, time.monotonic())


def run_transfer(sock, peer, transfer, *, window_size, loss, reorder, seed):
    channel = UnreliableChannel(
        sock, peer, loss=loss, reorder=reorder, seed=seed, emit=show_event
    )
    window = SenderWindow(
        transfer.packets, window_size=window_size, send_packet=channel.send, emit=show_event
    )
    print(f"File: {transfer.file_name} ({transfer.file_size} bytes)")
    print(f"Packets: {len(transfer.packets) - 1} DATA, {len(transfer.packets)} including END")
    print(f"Window: {window_size} | loss: {loss:g} | reorder: {reorder:g} | seed: {seed}")
    print(f"Sender port: {sock.getsockname()[1]} | receiver: {peer[0]}:{peer[1]}")

    window.fill_window(time.monotonic())
    while not window.done():
        read_acks(sock, peer, window)
        channel.tick(time.monotonic())
        window.on_tick(time.monotonic())
        window.fill_window(time.monotonic())

    # A duplicate attempt can still be held when the final ACK arrives.
    while channel.has_pending():
        channel.tick(time.monotonic())
        if channel.has_pending():
            time.sleep(POLL_INTERVAL)

    sender, network = window.stats(), channel.stats()
    print("----- SENDER SUMMARY -----")
    print(f"Transmission attempts: {sender['transmissions']} | UDP sends: {network['udp_sent']}")
    print(f"Dropped: {network['dropped']} | reordered: {network['reordered']} | flushed: {network['flushed']}")
    print(
        f"Retransmissions: {sender['retransmissions']} "
        f"({sender['fast_retransmits']} fast, {sender['timeouts']} timeout)"
    )
    print(f"Acknowledged: {sender['acked_packets']} | transfer time: {sender['transfer_time_s']:.3f}s")
    print("TRANSPORT COMPLETE; hash result shown at receiver")


def main():
    parser = argparse.ArgumentParser(description="Send one file reliably over UDP.")
    parser.add_argument("file", help="path of the file to send")
    parser.add_argument("--host", default="127.0.0.1", help="receiver IPv4 address or hostname")
    parser.add_argument("--port", type=int, default=9000, help="receiver UDP port")
    parser.add_argument("--window", type=int, default=8, help="maximum outstanding packets")
    parser.add_argument("--loss", type=float, default=0.1, help="simulated loss probability")
    parser.add_argument("--reorder", type=float, default=0.1, help="simulated hold probability")
    parser.add_argument("--seed", type=int, help="seed for repeatable simulator decisions")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if not 1 <= args.window <= MAX_WINDOW:
        parser.error(f"--window must be between 1 and {MAX_WINDOW}")
    if not 0 <= args.loss <= 1 or not 0 <= args.reorder <= 1:
        parser.error("--loss and --reorder must be probabilities between 0 and 1")

    try:
        peer = (socket.gethostbyname(args.host), args.port)
        transfer = prepare_transfer(args.file)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(("0.0.0.0", 0))
            run_transfer(
                sock, peer, transfer, window_size=args.window,
                loss=args.loss, reorder=args.reorder, seed=args.seed,
            )
    except KeyboardInterrupt:
        print("\nSender stopped before completion.")
        return 130
    except (OSError, ValueError) as error:
        parser.exit(1, f"Sender error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
