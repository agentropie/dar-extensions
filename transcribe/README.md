# transcribe

Local speech-to-text for dar agents using Cactus Whistle. Accepts WAV and
single-stream Ogg/Opus voice notes; decoding and inference stay on this machine.
The extension is inactive unless `extensions.transcribe` is present and enabled.
It does not enable transcription automatically in any chat extension.

## Setup

Vendor this crate into `extensions/transcribe/`, then run `dar build`.
It has no `requires_stock` and is not an agent singleton.

```yaml
extensions:
  transcribe:
    enabled: true
    language: fr            # omit for automatic language detection
    model_dir: data/stt     # persistent; relative to the agent root
    keywords: [Atlas, Boreal]
    max_audio_secs: 900
    max_file_mb: 25         # MiB (1024 * 1024 bytes)
    timeout_secs: 600
```

Languages: `en`, `de`, `fr`, `es`, `it`, `nl`, `pl`. Unknown configuration keys,
unsupported languages, absolute/parent-traversing model directories and zero
bounds fail registration. Keywords are trimmed, deduplicated and must be nonempty
single lines. There is no numeric keyword limit.

The agent calls:

```json
{"path":"data/voice-note.ogg","keywords":["Atlas","Élodie"]}
```

`transcribe_audio` returns JSON text:

```json
{"text":"Bonjour Atlas.","duration_secs":4.17,"windowed":false,"language":"fr"}
```

`language` can be null if the helper does not report it. `windowed` is true above
30 seconds: the engine uses successive windows and sentence boundaries.
Names, addresses and amounts can be wrong even with keyword biasing; verify
important details against the recording. Call keywords augment config keywords.
Whistle is best suited to short voice notes; accuracy drops on recordings over a few minutes, and names or amounts can be wrong or missing late in long audio.
Paths must resolve inside the agent root; directories and escaping symlinks are
rejected. WAV input is passed unchanged to the helper after header validation;
PCM8/16/24/32 and float32/64 with arbitrary sample rates/channels are accepted.
Ogg Opus is decoded to 16 kHz mono with pre-skip, output gain and final trimming.
Chained/multiplexed Ogg streams and other codecs are unsupported. Complete
WhatsApp Ogg pages without an EOS flag and with an initial granule offset are
accepted; truncated pages are rejected.

Failures are tool outcomes with stable codes: `invalid_args`, `not_found`,
`unsupported_format`, `too_large`, `too_long`, `busy`/`timeout`,
`model_unavailable`, `engine_failed`. Unsupported OS/architecture returns
`model_unavailable` with a clear unsupported-platform message. Only Linux
`aarch64` and `x86_64` are supported; the extension still registers elsewhere.

## Persistence, execution and bounds

Model/helper files, advisory locks and per-call `tmp/` live under `model_dir`.
Startup clears stale temp files under the transcription lock and launches artifact
preparation in the background. A download failure logs one clear line and leaves the
host running. The MCP bridge never runs `start()`: every tool call also ensures
artifacts and retries a failed initial download.

Independent host/bridge processes use download and transcription file locks.
Downloads follow HTTPS redirects, use a 15-second connection timeout and a
180-second request timeout, stream with the pinned size cap, and verify size plus
SHA-256 before fsync and atomic rename. Invalid artifacts are repaired; valid ones
are reused. Full hashing occurs on first use in each process; later uses check
size. Same-size tampering after that first hash is detected on the next process
start, not on every call. The helper is executable (`0755`). Partial files are
removed on failure/cancellation and repaired after an unclean shutdown.

One transcription runs at a time across bridges. The overall deadline includes
lock waiting, decoding, artifact preparation and helper execution. Decoding and
WAV preparation run off the async event loop; the helper is killed and reaped on
timeout. Per-call temp files are removed on success, failure or cancellation.
Helper stdout is bounded at 4 MiB (oversize output is rejected), stderr at 32 KiB
retained while both pipes are drained. No threads override is passed.

The 600-second default allows margin over the observed roughly 0.35 real-time
factor for long clips. Surrounding clients/surfaces can expire sooner: stock dar's
MCP proxy has a 120-second call timeout; `whatsapp-web` defaults to a 300-second
turn timeout. Codex's chat adapter uses a 30-second request/reply timeout (not a
proven whole-tool-call bound), while Pi has no explicit production turn timeout
in the checked adapter. Configure the client/surface timeout too, or use shorter
recordings; 15-minute support here does not guarantee one chat turn lasts that
long. These observations used dar revision
`9c6bfdfd24f37a07f521e98be8c1ef04782ced99`.

## Sandbox and privacy

Use `sandboxed: true` and the normal Debian bookworm sandbox image (glibc 2.36).
The downloaded helper requires glibc >= 2.27. It executes from the persistent
`data/` bind mount, which must allow execution. Docker's `/tmp` tmpfs can be
`noexec`; neither the helper nor scratch audio is placed there. The extension
supports a read-only root and ephemeral `HOME`; only `model_dir` is written.
No ffmpeg, system libopus, pkg-config, or changes to dar's build image are needed:
`ogg` is pure Rust and `opus` uses `opusic-sys`'s bundled C source, built statically
with CMake and the existing Alpine build packages.

The extension's only network activity is downloading the pinned model/helper.
It sends no audio or transcript to a network service. Once artifacts are cached,
transcription works with networking disabled. The transcript is returned to the
agent and then follows its normal chat-backend/LLM-provider path, outside this
extension. The host MCP bridge separately logs redacted/truncated tool-call
summaries, so a transcript prefix may appear in host logs. Extension logs contain
no audio/transcript, only outcomes, duration and diagnostic paths/stderr.

The helper is a vendor-supplied, binary-only (closed-source engine implementation)
executable run with the agent's own privileges, not an additional security sandbox.
The published Apache-2.0 license applies to the distributed binaries; their C/C++
engine source is not published in the linked Python repository. Runtime download
does not bundle or redistribute the engine or weights in this crate.

## Provenance and licenses

| Artifact | Pinned revision | Bytes | SHA-256 |
|---|---|---:|---|
| [Whistle model](https://huggingface.co/Cactus-Compute/whistle/resolve/741118687e1f008f06d1351a402a731274789790/whistle.cact) | `741118687e1f008f06d1351a402a731274789790` | 16,919,407 | `b6e02f048568ac5d01a2042556c658061e699acbc0aa2a1439f52f3d461dffeb` |
| [Linux arm64 helper](https://huggingface.co/Cactus-Compute/needle3/resolve/d6c7d6a2e9655505e0f8e23339356dd1705900eb/linux-arm64/needle) | `d6c7d6a2e9655505e0f8e23339356dd1705900eb` | 1,439,720 | `a7dd07f740763507edbca760dc7e7065b6da6b6bd6c3a05288ca974f14ce0fcb` |
| [Linux x86_64 helper](https://huggingface.co/Cactus-Compute/needle3/resolve/d6c7d6a2e9655505e0f8e23339356dd1705900eb/linux-x86_64/needle) | `d6c7d6a2e9655505e0f8e23339356dd1705900eb` | 1,550,176 | `77cd35ee75c53df8532b397e11ed7cf94a45d89a9f14f50881fd9c68ba54a2d0` |

Licenses: [engine binaries, Apache-2.0](https://huggingface.co/Cactus-Compute/needle3/raw/d6c7d6a2e9655505e0f8e23339356dd1705900eb/LICENSE),
[Whistle weights card, Apache-2.0](https://huggingface.co/Cactus-Compute/whistle/raw/741118687e1f008f06d1351a402a731274789790/README.md),
[published Python source, Apache-2.0](https://github.com/cactus-compute/needle/blob/278eea197d6a89a274d5505de757f5a3acb7ecfa/LICENSE).
The checked source tree at `278eea197d6a89a274d5505de757f5a3acb7ecfa` contains
Python and setup/test assets, no C/C++ engine source. The static engine archive
requires libc++ and glibc symbols; this extension uses the pinned glibc CLI to
keep the dar-sandbox binary static musl without changing the build image.

`SttEngine` isolates inference from the tool, download and audio modules. The
known future fallback is Parakeet TDT 0.6B v3 Q8 through transcribe.cpp 0.4.1
(source pin `5700f127e38dd53c4735ea23ef743b4b538eacd4`, model revision
`d8d8c32cb2017c57e5307767343e97870f8a7f53`); it is not implemented here.

## Tests

```bash
cargo test --manifest-path transcribe/Cargo.toml --locked
```

Tests use synthetic committed audio and local fake download/helper adapters;
none accesses the network or uses private recordings. Native development needs
CMake for bundled libopus. See `tests/fixtures/README.md` for fixture provenance.
