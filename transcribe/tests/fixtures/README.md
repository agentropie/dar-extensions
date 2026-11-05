# Synthetic audio

`tone.ogg` contains a generated 440 Hz sine wave, amplitude 8000 in signed
16-bit PCM, for exactly 0.5 seconds (24,000 samples at 48 kHz, mono).
It contains no speech or recording of a person.

It was encoded with `opus` 0.4.0 / bundled libopus from `opusic-sys` 0.7.7,
using the Audio application, 32 kbit/s, 20 ms packets, and `ogg` 0.9.2.
The OpusHead pre-skip is 312 samples at 48 kHz; the final granule is 24,312.
The last packet contains padding, so decoding must trim both pre-skip and
the end to yield 8,000 samples at 16 kHz.

Size: 2,997 bytes. SHA-256:
`08714f4f6081b78774762ab63b15f7a076313057f35cb45c08d0a85d62939ea1`.
