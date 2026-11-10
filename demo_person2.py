# Person 2 demonstration for Ali Hadi Zbeeb; generated with OpenAI Codex assistance.
"""Run a deterministic in-memory explanation of window recovery, without sockets."""
import argparse
import json

from tests.harness import Harness


def render(event):
    time = event["time"]
    side = event["side"]
    name = event["event"]
    if name == "WINDOW":
        slots = " ".join(f'{p["seq"]}:{p["state"]}' for p in event["packets"])
        details = f'base={event["base"]} next={event["next_seq"]} [{slots}]'
    else:
        details = " ".join(f"{k}={v}" for k, v in event.items()
                           if k not in {"time", "side", "event", "base", "next_seq"})
    print(f"{time:6.2f}s | {side:8} | {name:14} | {details}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", nargs="?", default="fast",
                        choices=["clean", "fast", "timeout", "two-gaps", "end-loss",
                                 "final-ack-loss", "reorder", "empty"])
    args = parser.parse_args()
    data = bytes(range(256)) * 24
    options = {}
    if args.scenario == "fast":
        options["drops"] = {(0, 1)}
    elif args.scenario == "timeout":
        options.update(window_size=1, drops={(0, 1)})
    elif args.scenario == "two-gaps":
        data = bytes(range(256)) * 20
        options.update(window_size=6, drops={(0, 1), (4, 1)})
    elif args.scenario == "end-loss":
        options["drops"] = {(6, 1)}
    elif args.scenario == "final-ack-loss":
        options["drop_final_ack"] = True
    elif args.scenario == "reorder":
        options["reorder_first"] = True
    elif args.scenario == "empty":
        data = b""
    print("PERSON 2 IN-MEMORY DEMO | simulated seconds | no real UDP/file I/O")
    print(f"scenario={args.scenario} bytes={len(data)} DATA={(len(data)+1023)//1024} "
          f"N={options.get('window_size', 4)} scripted_settings={options}")
    h = Harness(data, emit=render, **options).run()
    print("Sender:", json.dumps(h.window.stats(), indent=2))
    print("Reference receiver:", h.verification)
    assert bytes(h.output) == data and h.verification == "MATCH"


if __name__ == "__main__":
    main()
