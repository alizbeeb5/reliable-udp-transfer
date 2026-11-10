# Ali's Person 2 guide — EECE 350

This guide explains the initial AI-assisted implementation. Read it alongside
`window.py` and run the examples. The other two production components are not yet
in this repository. The reference peers in `tests/harness.py` exist to test your
component, not to claim your teammates' work is complete.

## 1. What the whole project does

UDP sends datagrams but does not itself recover a lost datagram or put reordered
datagrams back in order. Your group adds that behavior above UDP to transfer a file.

| Owner | Input | Job | Output |
| --- | --- | --- | --- |
| Person 1 | Original file | Split bytes, encode packets, calculate hash | Ordered tuple of DATA packets followed by END |
| You: Person 2 | That tuple, channel callback, incoming ACK numbers | Decide when to send and what to resend | Calls to the channel plus window/events/statistics |
| Person 1's channel | Every transmission request | Simulate loss/reordering; use UDP socket | Datagrams, some lost or delayed |
| Person 3 | Received datagrams | Buffer gaps, write in order, check hash | Cumulative ACKs and final MATCH/MISMATCH |

Your module does not read the file or open a socket. It treats a packet as bytes
and submits those exact same bytes again when necessary. The channel decides
what actually leaves the socket. The module cannot use a simulator's knowledge
of a drop; it has to infer missing data from ACKs and timeouts.

The custom header is `!BIH`: one-byte type, four-byte packet sequence, two-byte
payload length. UDP already carries source/destination ports; your header has
no extra port fields. Sequences count packets, not bytes. A 1024-byte packet and
a one-byte packet each consume one sequence number and one window slot.

## 2. The three numbers that drive your window

`base` is the oldest unacknowledged packet. `next_seq` is the next packet never
submitted to the channel. `window_size`, or N, is the maximum number of unique
outstanding packets.

Suppose N = 4, base = 2, next_seq = 5:

| Sequences | Meaning |
| --- | --- |
| 0, 1 | Acknowledged; no further retry needed |
| 2, 3, 4 | Submitted, waiting for a cumulative ACK |
| 5 | Allowed to send now; final free slot in [2, 5] |
| 6 onward | Cannot send yet |

The key conditions are:

```python
0 <= base <= next_seq <= len(packets)
next_seq - base <= window_size
next_seq < min(base + window_size, len(packets))  # permission for a new send
```

`next_seq - base` counts outstanding unique packets, not transmission attempts.
Retransmitting packet 2 does not consume a second window slot or increase
`next_seq`. It is another attempt to deliver the same packet.

`fill_window(now)` repeats the permission check and submits new packets until
the window is full or every packet has been submitted. Calling it again cannot
resend anything: the already advanced `next_seq` prevents that.

## 3. ACK k means “I need packet k next”

ACK 5 confirms packets 0, 1, 2, 3, 4. It does NOT confirm packet 5.

Example with N = 4:

| Step | What happens | Sender consequence |
| --- | --- | --- |
| 1 | Send 0, 1, 2, 3; packet 1 is lost | base = 0, next_seq = 4 |
| 2 | Receiver accepts 0, sends ACK 1 | base becomes 1; packet 4 may now be sent |
| 3 | Receiver sees 2 and 3 but still needs 1 | Buffer them; repeat ACK 1 |
| 4 | Receiver sees 4 and still needs 1 | Third duplicate ACK 1 triggers retry of 1 |
| 5 | Receiver gets 1 and drains buffered 2, 3, 4 | ACK 5 acknowledges the whole contiguous run |
| 6 | Sender processes ACK 5 | base becomes 5; new window can be filled |

The ACK that first advanced base to 1 is not a duplicate. Three MORE ACK 1s
trigger fast retransmit. At startup the initial base is already zero, so three
received ACK 0s are sufficient even without an earlier ACK 0.

`on_ack(a, now)` has these cases:

| Condition | Action |
| --- | --- |
| `base < a <= next_seq` | Mark [base, a) acknowledged; move base; reset duplicate tracking |
| `a == base` and something is outstanding | Count a duplicate; possibly fast retransmit |
| `a < base` | Ignore stale ACK; do not disturb duplicate count or timer |
| `a > next_seq` or invalid integer | Ignore; it claims data never submitted |
| `a == base == next_seq` | Nothing outstanding; do not count duplicates |

The outer sender loop must first check the source address, decode/validate the
datagram, and require ACK type. An integer-only API cannot verify those things.

## 4. Two ways to decide a retry is necessary

**Timeout:** If the oldest outstanding packet has not been acknowledged before
the deadline, submit that packet again. Only `base` is retried. Buffered later
packets can become acknowledged when the gap is repaired.

**Fast retransmit:** Three duplicate ACKs for the current base suggest a gap.
Retry base immediately instead of waiting for timeout. Reordering can also cause
duplicate ACKs, so this is evidence of a likely gap, not proof of permanent loss.

`fast_retransmitted_base` prevents a stream of repeated equal ACKs from creating
a retransmission loop. Once that base has been fast-retransmitted, additional
duplicate ACKs cannot trigger another fast retry until base advances. If the
retry is lost, the timer still provides another recovery attempt.

This is not Go-Back-N: the sender never resends the entire window on expiry.
It uses cumulative ACKs, receiver buffering, and oldest-packet retries. There
are no individual selective ACKs, and this is not full TCP congestion control.

Run these and compare the event sequences:

```bash
python3 demo_person2.py fast
python3 demo_person2.py timeout
python3 demo_person2.py two-gaps
```

The two-gap case drops packets 0 and 4. Packets 1, 2, 3 produce three ACK 0s.
Retrying 0 advances the ACK to 4. There are insufficient later duplicate ACKs
to fast-retransmit 4, so its timer repairs that second gap.

## 5. Why the timeout adapts

RTT is the time from submitting a packet until its ACK. RTO is the time the
sender is willing to wait before retrying. RTT is a measurement; RTO is a policy.

The implementation uses the contract's simplified RFC 6298-inspired estimator.
All times are seconds; live calls supply `time.monotonic()`. Wall-clock changes
such as setting the system time must not change transfer deadlines.

Before any sample: RTO = 1 second. For the first eligible sample R:

```text
SRTT = R
RTTVAR = R / 2
```

For later eligible samples, with the OLD SRTT on the first line:

```text
RTTVAR = 0.75 * RTTVAR + 0.25 * abs(old_SRTT - R)
SRTT   = 0.875 * old_SRTT + 0.125 * R
RTO    = min(60, max(1, SRTT + max(0.01, 4 * RTTVAR)))
```

SRTT smooths the measured delay. RTTVAR measures variation so unstable delays
produce more safety margin. RTTVAR here is a smoothed absolute deviation,
not the statistical variance in squared seconds.

For R = 0.2 seconds: SRTT = 0.2, RTTVAR = 0.1, raw RTO = 0.6; clamp to 1.
For a second sample R = 0.3: RTTVAR stays 0.1, SRTT becomes 0.2125,
raw RTO = 0.6125; again clamp to 1. Fast local demos may therefore show RTO = 1
even though the estimates are updating correctly.

For the first sample R = 0.8, raw RTO = 2.4 seconds; no minimum clamp is needed.

Reference: https://www.rfc-editor.org/rfc/rfc6298.html, sections 2, 3, and 5.
The project omits TCP's handshake, congestion window and other TCP machinery.

## 6. Karn's rule and the single timer

If a packet was sent at 0.0, resent at 1.0, and ACKed at 1.2, the ACK does not
tell us whether the measured RTT should be 1.2 or 0.2. Therefore do not take an
RTT sample from a retransmitted packet. This avoids making the timeout falsely
small because a delayed original packet happened to be acknowledged after a retry.

The contract is additionally conservative: only sample ACKs advancing exactly
one packet, when that packet's attempt count is one. Skip cumulative jumps.
Timestamps start at channel submission, so simulator delay is part of the sample.

There is one `timer_deadline`:

| Event | Timer action |
| --- | --- |
| First outstanding submission | Start at now + RTO |
| Additional new sends | Keep existing deadline |
| Ordinary duplicate/stale ACK | Keep existing deadline |
| ACK advancing base, packets still outstanding | Restart at now + current RTO |
| ACK clears all currently outstanding packets | Stop timer |
| Fast retransmit | Restart at now + RTO; no doubling |
| Timeout | Retry base and restart using doubled RTO, capped at 60 |

The initial timeout sequence without fresh measurements is 1, 2, 4, 8, 16, 32,
60, 60 seconds. New eligible RTT measurements can bring the estimate down again.
An ACK for a retransmitted packet does not itself clear that backoff.

`on_tick(now)` compares the timestamp with the deadline and handles at most one
timeout per call. A delayed event loop does not send a burst for every missed
historical deadline. No timer thread is needed; single-loop ownership avoids
concurrent modifications of base and packet state.

Process available ACKs before checking expiry. If the third duplicate ACK causes
a fast retransmit in that iteration, its new deadline prevents a second timeout
retry immediately afterward.

## 7. END and the final ACK

For M DATA packets, sequences are 0 through M-1, END is M, and final ACK is M+1.
END carries the 32-byte raw SHA-256 digest. An empty file still has END 0 and
needs ACK 1.

Your controller treats END exactly like a DATA packet. It occupies a slot and
has retry state. Completion means `base == len(packets)`, never merely
`next_seq == len(packets)`.

If END arrives before a missing DATA packet, the receiver buffers it. It can
only finish after all earlier file bytes arrive. If the final ACK is lost,
your timer retries END. The receiver must remain running and repeat its final
ACK without rewriting or rehashing the file.

ACK confirms contiguous transport receipt, not hash success. The ACK format has
no hash-status bit. Your module reports:

> TRANSPORT COMPLETE; hash result shown at receiver

The receiver separately reports MATCH or MISMATCH. A mismatching digest still
gets the final transport ACK under the contract. Real receiver file-I/O errors
are fatal and withhold completion ACK.

## 8. Packet records, events, and counters

Each `_PacketState` stores attempts, first/last submission timestamps, and acked.
The visible state is ACKNOWLEDGED first, then RETRANSMITTED if attempts > 1,
then SENT if attempted once, otherwise UNSENT. The attempt history is retained
after acknowledgement.

`snapshot()` includes only the current window slots. Acknowledged packets leave
that window, so ACK events also identify the exact acknowledged range. Old
acknowledged slots do not remain in the active-window display.

`emit(event)` receives dictionaries with time, side, event and applicable details.
The terminal renderer in `demo_person2.py` shows how to format them. Event fields
and snapshots are copies so changing a display record cannot alter packet state.
Do not call controller methods from callbacks; callbacks must not re-enter it.

| Counter | What it counts |
| --- | --- |
| transmissions | Every channel submission, including simulated drops and holds |
| retransmissions | All submissions after a packet's first attempt |
| timeouts | Expirations triggering retries |
| fast_retransmits | Retries caused by three duplicate ACKs |
| acked_packets | base, including END at completion |
| transfer_time_s | First submission to final ACK; elapsed so far while running |

At successful completion, `transmissions = total_packets + retransmissions` and
`retransmissions = timeouts + fast_retransmits`. The channel separately owns drop,
reordering, actual UDP-send, and pending-hold counters. Do not confuse a channel
submission with an actual UDP send.

A socket/callback error propagates as a fatal local error. The caller should
abort with an incomplete/error report, rather than treating it as simulated loss.
At 100% loss the transfer cannot complete; the sender CLI must allow cancellation.

## 9. Read the code in this order

| Function | Question it answers |
| --- | --- |
| `__init__` | What must be valid before starting? What is the initial state? |
| `fill_window` | Which never-submitted packets are allowed now? |
| `_transmit` | How is one attempt recorded and passed to the channel? |
| `on_ack` | Does the ACK advance, repeat, or make an invalid/stale claim? |
| `_update_rtt` | How does a clean sample change the timeout estimate? |
| `on_tick` | Has the oldest outstanding packet timed out? |
| `done` | Has END actually been acknowledged? |
| `snapshot`, `stats`, `_log` | What does the display need to show? |

The constructor performs no sends. The first call to `fill_window` starts timing.
Packet input is copied to a tuple of immutable bytes; packet headers are assumed
validated/prepared by Person 1. This module intentionally does not parse them.

Memory is O(P) for P packets and per-packet records, as the contract prepares
the whole transfer up front. Snapshot construction is O(N). A cumulative ACK
visits the newly acknowledged records; each packet is marked acknowledged once.

## 10. How your teammates connect to your code

The following is integration code for Person 1's future sender CLI, not a command
that runs today: the production protocol, packetizer, and channel are pending.
`peer` must be the resolved IPv4 `(address, port)` used for the receiver. Socket
setup, startup settings, rendering, cancellation/error reporting, and final
summaries belong in the surrounding CLI.

```python
import select
import time
from protocol import ACK, decode_packet
from window import SenderWindow

# sock: the same bound UDP socket used by channel.send and incoming ACKs
# transfer: Person 1's PreparedTransfer
# channel: Person 1's UnreliableChannel
sock.setblocking(False)
window = SenderWindow(transfer.packets, window_size=N,
                      send_packet=channel.send, emit=emit)
window.fill_window(time.monotonic())

while not window.done():
    select.select([sock], [], [], 0.01)
    for _ in range(64):  # bounded ACK batch: do not starve timers/channel ticks
        try:
            datagram, addr = sock.recvfrom(65535)
        except BlockingIOError:
            break
        if addr != peer:
            continue
        try:
            packet = decode_packet(datagram)
        except ValueError:
            continue  # emit a malformed-datagram event in the production CLI
        if packet.packet_type == ACK:
            window.on_ack(packet.seq, time.monotonic())

    channel.tick(time.monotonic())
    window.on_tick(time.monotonic())
    window.fill_window(time.monotonic())

# Completion duration is already fixed at the final ACK.
while channel.has_pending():
    channel.tick(time.monotonic())
    time.sleep(0.01)
print(window.stats())
print(channel.stats())
```

`channel.send` must return normally on simulated loss; it does not report delivery
success. It must not synchronously call `window.on_ack`. Real ACKs are read later
in the loop. Every retry uses that same callback with the same packet bytes.

Temporary integration detail: `window.py` has `MAX_WINDOW = 4096` locally because
the repository has no `protocol.py` yet. Once Person 1 supplies the shared module,
replace this definition with `from protocol import MAX_WINDOW`, as the contract
specifies. The module's method signatures and wire assumptions stay unchanged.

## 11. What was tested and what remains

Run `python3 -m unittest discover -v`. The suite checks configuration, bounded
windows, cumulative jumps, duplicate/stale/impossible ACKs, one fast retry per
base, timer ordering, exponential backoff/cap, Karn's rule, exact RTT examples,
END completion, duration/counter correctness, and display-copy independence.

Transfer tests cover empty/binary/boundary-size files, scripted DATA and END loss,
final-ACK loss, two gaps, reordering, idle hold release with N=1, END buffered
behind a gap, and 40 deterministic seeded loss scenarios. They verify reconstructed
bytes and the reference receiver hash. All of these use in-memory peers and
artificial times. Active-transfer stats tests patch the clock consistently.

The actual UDP channel, production parser, real receiver disk writes, source-peer
filtering, actual receiver failure/cancellation handling, two-machine behavior,
and complete combined display still require integration tests with your teammates.
The test harness has only the behavior needed for these scenarios; it is not
a replacement for the full channel/receiver contract or their acceptance tests.

## 12. Oral practice

1. **Why a window?** To keep multiple packets in flight instead of waiting after
   each packet. N bounds outstanding unique packets.
2. **ACK 7 means what?** Packets 0 through 6 are contiguous; 7 is next needed.
3. **Why does a simulated drop still advance next_seq?** It is still a send
   attempt. Only an ACK can establish delivery.
4. **Why retry only base?** The receiver buffers later packets; repairing the
   first gap may cumulatively acknowledge them.
5. **Why three duplicate ACKs?** They suggest later packets arrived while the
   same earlier packet is missing, allowing earlier recovery than a timer.
6. **Can fast retransmit be unnecessary?** Yes, sufficient reordering can cause it.
7. **Why not keep retrying every three equal ACKs?** A duplicate stream would
   cause repeated unnecessary retries. The guard permits one fast retry per base;
   timeout still handles a lost retry.
8. **Why not reset the timer on every duplicate ACK?** Repeated duplicates could
   postpone recovery indefinitely.
9. **Why Karn's rule?** Without timestamps on the wire, an ACK cannot identify
   which transmission attempt it acknowledges.
10. **Why does timeout double?** Backoff avoids aggressive repeated retries when
    the path is delayed or unavailable.
11. **Where do you send over UDP?** Nowhere in `window.py`; Person 1's channel does.
12. **How do you know END succeeded?** Only when final ACK advances base to P.
13. **Does final ACK prove the hash matches?** No; the receiver reports that separately.
14. **What if final ACK is lost?** Retry END; completed receiver must re-ACK it.
15. **What if ACK jumps beyond next_seq?** Ignore it; it claims unsent data.
16. **What changes for N=1?** It becomes one outstanding packet at a time;
    recovery usually relies on timeout because there are no later packets to
    generate three duplicate ACKs.
17. **Is N TCP's congestion window?** No; it is a configured project window.
18. **Can permanent total loss complete?** No. Report incomplete on cancellation.

Practice modifying the window size and predicting the next base/next_seq before
running a demo. Then explain each TIMEOUT and RETRANSMIT line without reading
this guide. Keep AI provenance and actual student contributions accurate in
`AI_USAGE.md` as you review and adapt the implementation.
