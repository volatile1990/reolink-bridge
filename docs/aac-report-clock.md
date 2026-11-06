# AAC sender-report clock

The 6 November 2026 port starts from the exact deployed C9 source
`8473f37551c9c21a20bc1f2d723969346ac314d8`, retaining its CPU, socket-write and
durable-journal optimizations. It imports the reviewed V2 report clock and
raw/matched/GOP adapters; it does not replace the separate Eingang source or
change media payload, pacing, encoder settings or the native 4 ms fMP4 limit.
Source/build tests alone are not a production acceptance result. Deployment
starts with Hintertuer only and still needs actual RTP/RTCP, native fMP4 audio,
fresh recording, archive, preservation and natural-monitor checks.

Sender Reports associate RTP and NTP timestamps with the same instant, using
the maintained RTP-counter/realtime relationship
([RFC 3550, section 6.4.1](https://www.rfc-editor.org/rfc/rfc3550.html#section-6.4.1)).
The old raw adapter changed its inverse audio phase as arrival estimates moved;
the old GOP adapter exposed its per-GOP pacing correction in that mapping.
Both could publish millisecond phase changes despite uninterrupted 1024-sample
AAC steps. The sender-report clock below separates that report relationship
from packet arrival and the unchanged playout schedule.

The earlier V1 clock is excluded: a short 60-second history could hide small
persistent drift until its 250 ms protection bound was exhausted. V2 retains
bounded short and adaptive long evidence. Its tests include four simulated
eight-hour sessions, two four-day sessions and late pure delivery offsets;
these simulations do not establish physical multi-day camera stability.

The raw, matched and ordinary GOP AAC paths preserve original access-unit bytes,
1024-sample RTP increments and their existing playout schedules. Their report
mapping has a separate pump-owned clock. Arrival and scheduled-presentation
instants are estimates; they do not recover camera capture NTP.

An emitted clock starts at nominal sample rate after its first successfully sent
AU. A short clean TCP queue ramp cannot identify an oscillator rate, so there is
no short bootstrap fit. Every later report advances the retained double phase
using the previously committed frequency. Only after the complete paired report
has been sent does MarkReportSent commit that same phase and its next frequency.
The rounded wire counter is never the next phase anchor. Failed sends, peeks and
transport latency cannot substitute a later timestamp for the paired report.

The estimator holds at most60 short and60 long representatives, each a real
paired observation. The short scale uses one-second buckets selected by median
nominal-clock residual. Raw AAC contributes
only each original block's last AU and its actual block-arrival timestamp. The
other AUs are correlated samples from that same TCP receive, not independent
clock observations. Matched and GOP paths contribute their unchanged confirmed
planned presentation instants, also only at original block ends.

At least9 buckets and pair baselines of at least8 seconds are required. The
frequency is the median long-baseline pair slope. Its observed ambiguity is
max(1 sample,4 × maximum fit residual) divided by the fit span. The one-sample
floor covers tick/numerical resolution. The four-sided envelope is conservative
for a constant delivery-phase step: a line through a half-window step can have
slope≈2Δ/T while maximum residual≈Δ/2. A two-sided envelope undercovers nominal
rate and incorrectly calls that delivery step oscillator drift.

The long scale samples a completed short-bucket representative initially every
60 seconds. Once60 points are retained, each chronological pair is replaced by
its earlier REAL point and future sampling spacing doubles:60,120,240 seconds
and onward. The oldest observation survives; no averaged coordinate pair is
invented. Thus the memory limit does not impose a fixed maximum evidence span.
Persistent small drift can eventually exceed the observed arrival uncertainty
before the unchanged250ms guard. A fixed60-second or60-minute FIFO alone cannot
do this: it can leave a real rate inside uncertainty forever while phase grows.
The existing open-second buffer remains bounded at128 independent observations.

The two scales are fitted separately. Fit lines are cached by observation
revision, with the long fit recomputed only after a retained long point changes.
Repeated peeks and the paired MarkReportSent reuse the same line; only evaluating
its target at a new time is necessary. Diagnostics expose both fit counts, the
maximum120 representatives and adaptive long sampling spacing.

Once a mature long fit exists, a short contradictory fit cannot by itself
replace that prior: even a clean30-second delivery ramp can resemble oscillator
drift. A fast replacement additionally needs more than one AAC access unit
(1024 samples/64ms at16kHz) of accumulated displacement beyond the fast fit's
phase uncertainty. This is an observed evidence floor, not capture knowledge.
Initial acquisition retains the existing nine-bucket contract so genuine
4000ppm remains within its existing250ms/1500-second convergence budget.
Accepted frequency evidence becomes the retained rate prior. Long estimates
can resolve small drift without meeting the short replacement floor; a large
persistent current change can override old long history through that floor.
Both the original fast presentation guard and the selected long guard remain
250ms; old long history cannot conceal a current large presentation jump.

Nominal rate remains the prior until the observed frequency ambiguity excludes
it. Only then is nonnominal rate sticky evidence for this clock generation.
Phase correction is disabled before that evidence: a delivery-offset step or
startup ramp must keep the original nominal report line exactly. Once a genuine
rate difference is resolved, phase error outside observed residual uncertainty
may converge through future frequency, never through a phase reanchor. Phase
correction has a60-second time constant and a100ppm frequency cap; total
frequency movement is limited to100ppm per elapsed second and ±1% nominal.
An unsupported rate whose ambiguity lies wholly outside this range fails closed.
A mature fit more than250ms from the presentation estimate also fails before a
new report is emitted. Those are explicit acquisition bounds, not proof of
capture synchronization.

Epoch/format changes, actual RTP sample gaps and more than5 seconds between
successful sends start a new generation. Retrograde de-bursted arrival estimates
do not: two16-AU batches received100ms apart legitimately have overlapping
arrival windows. Existing GOP forward-recovery offsets remain paired with the
existing presentation pause; the report clock checks the exact modular offset,
retains its published phase/rate and discards the crossed fit window. GOP video
and audio reports use one captured monotonic-to-UTC origin. Diagnostics expose
acquisition, rate, fit span, observed uncertainty, presentation-offset uncertainty
and explicit resets for raw/matched and GOP paths.

## Contracts and acquisition

The45-minute raw fixture intentionally generates16064 samples per real second,
although its codec sample rate is16000. Sender-report inversion therefore uses
the known16064-Hz fixture oracle and signed local RTP differences across wrap.
Using nominal16000 incorrectly adds up to255µs from the latest AU's age alone.
The steady-state100µs bound is retained; acquisition is separately bounded at
250ms and must converge within1500 seconds. The run still spans45 minutes, RTP
wraps, the UTC jump, original packets and all reports. It does not promise
unknowable100µs accuracy immediately after a single unknown-rate AU.

Default ProtectTests includes four8-hour sessions with real±25/50ppm plus
alternating±8ms arrival jitter, two4-day±1ppm sessions with60-second SR cadence,
and an8-hour nominal source with late±8/24ms delivery steps and ramps. Genuine
1024-sample AU progression and timestamp wraps remain, with exact phase equality
before/after MarkReportSent, maximum250ms acquisition and bounded representative
count. The same new tests failed against the unchanged V1 core; the original
negative log is retained outside the source worktree.

Default ProtectTests also includes the clock, raw/matched adapter jitter, two-batch
burst and pure delivery-step/ramp regressions through LowQualityRtcpTests.RunAll.
GopAacContractTests additionally reproduces >5ms variation in the old phase
inverse while the new emitted line stays continuous, with identical AU bytes.
An actual RTSP loopback checks61 paired AAC reports through alternating TCP
bundle jitter, original payload/counters and a UTC jump. Its media/command barrier
waits for the complete AAC batch before advancing the controlled clock.
The loopback tests retain authentication, PAUSE/PLAY/TEARDOWN, cancellation,
unsent-anchor and forward-recovery contracts. Extra focused entry points are
--aac-report-clock-only, --aac-adapter-jitter-only, --aac-wire-jitter-only, --lq-rtcp-only and
--gop-aac-only; these do not replace the default run.

This corrects noisy RTP/report associations. It is not a promise that a native
muxer will produce zero fMP4 discontinuities: any real remaining sample-rate
difference or muxer quantization still requires its own strict media proof.
