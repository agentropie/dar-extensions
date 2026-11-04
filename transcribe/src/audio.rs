use std::fs::OpenOptions;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;

use crate::Failure;

#[derive(Debug)]
pub enum AudioData {
    Wav(Vec<u8>),
    Pcm16kMono(Vec<i16>),
}

#[derive(Debug)]
pub struct AudioInput {
    pub duration_secs: f64,
    pub audio: AudioData,
}

pub fn load(root: &Path, path: &str, max_bytes: u64, max_secs: f64) -> Result<AudioInput, Failure> {
    load_impl(root, path, max_bytes, max_secs, None)
}

pub(crate) fn load_until(
    root: &Path,
    path: &str,
    max_bytes: u64,
    max_secs: f64,
    deadline: Instant,
) -> Result<AudioInput, Failure> {
    load_impl(root, path, max_bytes, max_secs, Some(deadline))
}

fn load_impl(
    root: &Path,
    path: &str,
    max_bytes: u64,
    max_secs: f64,
    deadline: Option<Instant>,
) -> Result<AudioInput, Failure> {
    check_deadline(deadline)?;
    let root = root.canonicalize().map_err(|_| missing())?;
    let requested = Path::new(path);
    if requested
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
        || (requested.is_absolute() && !requested.starts_with(&root))
    {
        return Err(Failure::new(
            "invalid_args",
            "Audio path leaves the agent folder",
        ));
    }
    let path = root.join(requested).canonicalize().map_err(|_| missing())?;
    if !path.starts_with(&root) {
        return Err(Failure::new(
            "invalid_args",
            "Audio path leaves the agent folder",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(&path).map_err(|_| missing())?;
    let metadata = file.metadata().map_err(|_| missing())?;
    if !metadata.is_file() {
        return Err(Failure::new(
            "invalid_args",
            "Audio path must be a regular file",
        ));
    }
    if metadata.len() > max_bytes {
        return Err(Failure::new("too_large", "Audio file exceeds max_file_mb"));
    }
    let mut bytes = Vec::new();
    let mut remaining = max_bytes.saturating_add(1);
    let mut chunk = [0; 64 * 1024];
    while remaining > 0 {
        check_deadline(deadline)?;
        let limit = remaining.min(chunk.len() as u64) as usize;
        let count = file.read(&mut chunk[..limit]).map_err(|_| missing())?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        remaining -= count as u64;
    }
    check_deadline(deadline)?;
    if bytes.len() as u64 > max_bytes {
        return Err(Failure::new("too_large", "Audio file exceeds max_file_mb"));
    }
    let header: &[u8; 12] = bytes.get(..12).ok_or_else(unsupported)?.try_into().unwrap();
    if &header[..4] == b"OggS" {
        return opus_audio(Cursor::new(bytes), max_secs, deadline);
    }
    if &header[..4] != b"RIFF" || &header[8..] != b"WAVE" {
        return Err(unsupported());
    }
    let mut cursor = Cursor::new(bytes.as_slice());
    cursor.set_position(12);
    let duration_secs = wav_duration(&mut cursor, header, bytes.len() as u64, deadline)?;
    if duration_secs > max_secs {
        return Err(Failure::new(
            "too_long",
            "Audio duration exceeds max_audio_secs",
        ));
    }
    Ok(AudioInput {
        duration_secs,
        audio: AudioData::Wav(bytes),
    })
}

fn opus_audio(
    source: impl Read + Seek,
    max_secs: f64,
    deadline: Option<Instant>,
) -> Result<AudioInput, Failure> {
    let mut reader = ogg::reading::PacketReader::new(source);
    let head = reader.read_packet_expected().map_err(|_| unsupported())?;
    let bytes = &head.data;
    if bytes.len() != 19
        || &bytes[..8] != b"OpusHead"
        || !(1..=15).contains(&bytes[8])
        || !matches!(bytes[9], 1 | 2)
        || bytes[18] != 0
        || !head.first_in_stream()
        || head.last_in_stream()
    {
        return Err(unsupported());
    }
    let serial = head.stream_serial();
    let preskip = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
    let gain = i16::from_le_bytes(bytes[16..18].try_into().unwrap());
    let tags = reader.read_packet_expected().map_err(|_| unsupported())?;
    if tags.stream_serial() != serial
        || tags.first_in_stream()
        || tags.last_in_stream()
        || tags.data.len() < 16
        || &tags.data[..8] != b"OpusTags"
    {
        return Err(unsupported());
    }
    let mut comments = &tags.data[8..];
    let vendor_len = comment_length(&mut comments)?;
    comments = comments.get(vendor_len..).ok_or_else(unsupported)?;
    let comment_count = comment_length(&mut comments)?;
    for _ in 0..comment_count {
        check_deadline(deadline)?;
        let length = comment_length(&mut comments)?;
        comments = comments.get(length..).ok_or_else(unsupported)?;
    }
    let mut decoder = opus::Decoder::new(16000, opus::Channels::Mono).map_err(|_| unsupported())?;
    decoder
        .set_gain(i32::from(gain))
        .map_err(|_| unsupported())?;
    let mut samples = Vec::new();
    let mut frame = [0i16; 1920]; // Opus's maximum 120 ms packet at 16 kHz.
    let mut final_granule = None;
    let mut initial_offset = None;
    let mut ended = false;
    let mut trimmed_page = false;
    let max_samples = (max_secs * 16000.0).floor() as usize;
    while let Some(packet) = reader.read_packet().map_err(|_| unsupported())? {
        check_deadline(deadline)?;
        if packet.stream_serial() != serial || packet.first_in_stream() || ended || trimmed_page {
            return Err(unsupported());
        }
        let count = decoder
            .decode(&packet.data, &mut frame, false)
            .map_err(|_| unsupported())?;
        if samples.len().saturating_add(count) > max_samples.saturating_add(preskip / 3 + 1920) {
            return Err(Failure::new(
                "too_long",
                "Audio duration exceeds max_audio_secs",
            ));
        }
        samples.extend_from_slice(&frame[..count]);
        if packet.last_in_page() {
            let granule = packet.absgp_page();
            if granule == u64::MAX || final_granule.is_some_and(|previous| granule < previous) {
                return Err(unsupported());
            }
            let decoded = samples.len() as u64 * 3;
            let offset = *initial_offset.get_or_insert_with(|| granule.saturating_sub(decoded));
            let expected = decoded.checked_add(offset).ok_or_else(unsupported)?;
            if granule > expected {
                return Err(unsupported());
            }
            trimmed_page = granule < expected;
            final_granule = Some(granule);
        }
        ended = packet.last_in_stream();
    }
    let granule = final_granule
        .and_then(|granule| granule.checked_sub(initial_offset.unwrap_or(0)))
        .ok_or_else(unsupported)?;
    if granule < preskip as u64 || granule > samples.len() as u64 * 3 {
        return Err(unsupported());
    }
    let duration_secs = (granule - preskip as u64) as f64 / 48000.0;
    if duration_secs > max_secs {
        return Err(Failure::new(
            "too_long",
            "Audio duration exceeds max_audio_secs",
        ));
    }
    let length = ((granule - preskip as u64) / 3) as usize;
    if length == 0 {
        return Err(unsupported());
    }
    samples.drain(..preskip / 3);
    samples.truncate(length);
    Ok(AudioInput {
        duration_secs,
        audio: AudioData::Pcm16kMono(samples),
    })
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), Failure> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(Failure::new("timeout", "Transcription timed out"));
    }
    Ok(())
}

fn comment_length(bytes: &mut &[u8]) -> Result<usize, Failure> {
    let value = bytes.get(..4).ok_or_else(unsupported)?;
    let length = u32::from_le_bytes(value.try_into().unwrap()) as usize;
    *bytes = &bytes[4..];
    Ok(length)
}

fn missing() -> Failure {
    Failure::new("not_found", "Audio file is missing or unreadable")
}

fn unsupported() -> Failure {
    Failure::new(
        "unsupported_format",
        "Unsupported or unreadable audio format",
    )
}

fn wav_duration(
    file: &mut (impl Read + Seek),
    header: &[u8; 12],
    size: u64,
    deadline: Option<Instant>,
) -> Result<f64, Failure> {
    let end = u64::from(u32::from_le_bytes(header[4..8].try_into().unwrap())) + 8;
    if end != size || end < 12 {
        return Err(unsupported());
    }
    let mut rate = None;
    let mut data = None;
    while file.stream_position().map_err(|_| unsupported())? + 8 <= end {
        check_deadline(deadline)?;
        let mut chunk = [0; 8];
        file.read_exact(&mut chunk).map_err(|_| unsupported())?;
        let length = u32::from_le_bytes(chunk[4..].try_into().unwrap()) as u64;
        let start = file.stream_position().map_err(|_| unsupported())?;
        if start + length > end {
            return Err(unsupported());
        }
        if &chunk[..4] == b"fmt " {
            if length < 16 || rate.is_some() {
                return Err(unsupported());
            }
            let mut fmt = [0; 16];
            file.read_exact(&mut fmt).map_err(|_| unsupported())?;
            let format = u16::from_le_bytes(fmt[0..2].try_into().unwrap());
            let channels = u16::from_le_bytes(fmt[2..4].try_into().unwrap()) as u32;
            let hz = u32::from_le_bytes(fmt[4..8].try_into().unwrap());
            let byte_rate = u32::from_le_bytes(fmt[8..12].try_into().unwrap());
            let align = u16::from_le_bytes(fmt[12..14].try_into().unwrap()) as u32;
            let bits = u16::from_le_bytes(fmt[14..16].try_into().unwrap());
            if channels == 0
                || hz == 0
                || !matches!((format, bits), (1, 8 | 16 | 24 | 32) | (3, 32 | 64))
                || align != channels * u32::from(bits / 8)
                || hz.checked_mul(align) != Some(byte_rate)
            {
                return Err(unsupported());
            }
            rate = Some((byte_rate, align));
        } else if &chunk[..4] == b"data" {
            if data.is_some() {
                return Err(unsupported());
            }
            data = Some(length);
        }
        file.seek(SeekFrom::Start(start + length + length % 2))
            .map_err(|_| unsupported())?;
    }
    match (rate, data) {
        (Some((byte_rate, align)), Some(length))
            if length > 0 && length % u64::from(align) == 0 =>
        {
            Ok(length as f64 / f64::from(byte_rate))
        }
        _ => Err(unsupported()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    fn wav(samples: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&(36 + samples * 2).to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&16000u32.to_le_bytes());
        bytes.extend_from_slice(&32000u32.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&16u16.to_le_bytes());
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&(samples * 2).to_le_bytes());
        bytes.resize(44 + samples as usize * 2, 0);
        bytes
    }

    #[test]
    fn wav_is_sniffed_by_content_and_reports_duration() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("clip.bin");
        std::fs::write(&path, wav(24000)).unwrap();
        let input = load(root.path(), "clip.bin", 100000, 900.0).unwrap();
        assert_eq!(input.duration_secs, 1.5);
        assert!(matches!(input.audio, AudioData::Wav(bytes) if bytes == wav(24000)));
    }

    #[test]
    fn synthetic_opus_decodes_to_mono_16k_with_preskip_and_end_trim() {
        let root = tempfile::tempdir().unwrap();
        std::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tone.ogg"),
            root.path().join("tone.dat"),
        )
        .unwrap();
        let input = load(root.path(), "tone.dat", 100000, 900.0).unwrap();
        assert_eq!(input.duration_secs, 0.5);
        let AudioData::Pcm16kMono(samples) = input.audio else {
            panic!("Opus must decode to PCM")
        };
        assert_eq!(samples.len(), 8000);
        assert!(samples.iter().any(|sample| sample.unsigned_abs() > 1000));
    }

    #[test]
    fn malformed_opus_comment_header_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let fixture = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tone.ogg"
        ))
        .unwrap();
        let mut reader = ogg::reading::PacketReader::new(std::io::Cursor::new(fixture));
        let file = File::create(root.path().join("invalid.ogg")).unwrap();
        let mut writer = ogg::writing::PacketWriter::new(file);
        while let Some(mut packet) = reader.read_packet().unwrap() {
            if packet.data.starts_with(b"OpusTags") {
                packet.data[8..12].copy_from_slice(&999u32.to_le_bytes());
            }
            let end = if packet.last_in_stream() {
                ogg::writing::PacketWriteEndInfo::EndStream
            } else {
                ogg::writing::PacketWriteEndInfo::EndPage
            };
            let serial = packet.stream_serial();
            let granule = packet.absgp_page();
            writer
                .write_packet(packet.data, serial, end, granule)
                .unwrap();
        }
        drop(writer);
        assert_eq!(
            load(root.path(), "invalid.ogg", 100000, 900.0)
                .unwrap_err()
                .code,
            "unsupported_format"
        );
    }

    #[test]
    fn paths_are_contained_and_must_name_a_regular_file() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let absolute = outside.path().to_str().unwrap();
        assert_eq!(
            load(root.path(), absolute, 100000, 900.0).unwrap_err().code,
            "invalid_args"
        );
        let traversal = format!(
            "../{}",
            outside.path().file_name().unwrap().to_str().unwrap()
        );
        assert_eq!(
            load(root.path(), &traversal, 100000, 900.0)
                .unwrap_err()
                .code,
            "invalid_args"
        );
        assert_eq!(
            load(root.path(), ".", 100000, 900.0).unwrap_err().code,
            "invalid_args"
        );
        assert_eq!(
            load(root.path(), "missing.wav", 100000, 900.0)
                .unwrap_err()
                .code,
            "not_found"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_cannot_escape_the_agent_root() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("escaped.wav")).unwrap();
        assert_eq!(
            load(root.path(), "escaped.wav", 100000, 900.0)
                .unwrap_err()
                .code,
            "invalid_args"
        );
        std::fs::write(root.path().join("clip.wav"), wav(1600)).unwrap();
        std::os::unix::fs::symlink("clip.wav", root.path().join("inside.wav")).unwrap();
        assert_eq!(
            load(root.path(), "inside.wav", 100000, 900.0)
                .unwrap()
                .duration_secs,
            0.1
        );
    }

    #[test]
    fn size_and_duration_limits_reject_before_transcription() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("clip.wav"), wav(24000)).unwrap();
        assert_eq!(
            load(root.path(), "clip.wav", 48043, 900.0)
                .unwrap_err()
                .code,
            "too_large"
        );
        assert_eq!(
            load(root.path(), "clip.wav", 48044, 1.0).unwrap_err().code,
            "too_long"
        );
        assert_eq!(
            load(root.path(), "clip.wav", 48044, 1.5)
                .unwrap()
                .duration_secs,
            1.5
        );
        std::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tone.ogg"),
            root.path().join("tone.ogg"),
        )
        .unwrap();
        assert_eq!(
            load(root.path(), "tone.ogg", 100000, 0.4).unwrap_err().code,
            "too_long"
        );
        assert_eq!(
            load(root.path(), "tone.ogg", 100000, 0.01)
                .unwrap_err()
                .code,
            "too_long"
        );
    }

    #[test]
    fn unsupported_and_truncated_files_report_format_errors() {
        let root = tempfile::tempdir().unwrap();
        let mut unsupported_wav = wav(1600);
        unsupported_wav[20..22].copy_from_slice(&7u16.to_le_bytes());
        let cases = [
            b"an invented non-audio string".to_vec(),
            b"RIFF".to_vec(),
            wav(1600)[..100].to_vec(),
            unsupported_wav,
        ];
        for bytes in cases {
            std::fs::write(root.path().join("bad.wav"), bytes).unwrap();
            assert_eq!(
                load(root.path(), "bad.wav", 100000, 900.0)
                    .unwrap_err()
                    .code,
                "unsupported_format"
            );
        }
    }

    #[test]
    fn ogg_requires_one_complete_uncorrupted_logical_stream() {
        let root = tempfile::tempdir().unwrap();
        let fixture = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tone.ogg"
        ))
        .unwrap();
        let mut chained = fixture.clone();
        chained.extend_from_slice(&fixture);
        let mut corrupt = fixture.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        for bytes in [chained, corrupt, fixture[..fixture.len() - 20].to_vec()] {
            std::fs::write(root.path().join("bad.ogg"), bytes).unwrap();
            assert_eq!(
                load(root.path(), "bad.ogg", 100000, 900.0)
                    .unwrap_err()
                    .code,
                "unsupported_format"
            );
        }
    }

    #[test]
    fn wav_duration_uses_the_recorded_rate_channels_and_sample_width() {
        let root = tempfile::tempdir().unwrap();
        for (format, bits, channels, hz, align, byte_rate, duration) in [
            (1u16, 8u16, 1u16, 16000u32, 1u16, 16000u32, 3.0),
            (1, 24, 1, 16000, 3, 48000, 1.0),
            (1, 32, 2, 48000, 8, 384000, 0.125),
            (3, 32, 2, 48000, 8, 384000, 0.125),
            (3, 64, 1, 16000, 8, 128000, 0.375),
        ] {
            let mut bytes = wav(24000);
            bytes[20..22].copy_from_slice(&format.to_le_bytes());
            bytes[22..24].copy_from_slice(&channels.to_le_bytes());
            bytes[24..28].copy_from_slice(&hz.to_le_bytes());
            bytes[28..32].copy_from_slice(&byte_rate.to_le_bytes());
            bytes[32..34].copy_from_slice(&align.to_le_bytes());
            bytes[34..36].copy_from_slice(&bits.to_le_bytes());
            std::fs::write(root.path().join("clip.wav"), bytes).unwrap();
            assert_eq!(
                load(root.path(), "clip.wav", 100000, 900.0)
                    .unwrap()
                    .duration_secs,
                duration
            );
        }
    }

    #[test]
    fn duplicate_wav_format_or_data_chunks_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let original = wav(1600);
        for extra in [&original[12..36], &original[36..]] {
            let mut bytes = original.clone();
            bytes.extend_from_slice(extra);
            let riff_size = (bytes.len() - 8) as u32;
            bytes[4..8].copy_from_slice(&riff_size.to_le_bytes());
            std::fs::write(root.path().join("ambiguous.wav"), bytes).unwrap();
            assert_eq!(
                load(root.path(), "ambiguous.wav", 100000, 900.0)
                    .unwrap_err()
                    .code,
                "unsupported_format"
            );
        }
    }

    #[test]
    fn wav_chunks_outside_the_declared_riff_container_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let original = wav(1600);
        let mut bytes = original.clone();
        bytes.extend_from_slice(&original[36..]);
        std::fs::write(root.path().join("trailing.wav"), bytes).unwrap();
        assert_eq!(
            load(root.path(), "trailing.wav", 100000, 900.0)
                .unwrap_err()
                .code,
            "unsupported_format"
        );
    }

    #[cfg(unix)]
    #[test]
    fn loaded_wav_survives_path_replacement_with_an_oversized_external_file() {
        let root = tempfile::tempdir().unwrap();
        let expected = wav(16);
        let path = root.path().join("clip.wav");
        std::fs::write(&path, &expected).unwrap();
        let loaded = load(root.path(), "clip.wav", 1000, 900.0).unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(outside.path(), wav(16000)).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(outside.path(), &path).unwrap();
        let AudioData::Wav(snapshot) = loaded.audio else {
            panic!("WAV must be preserved unchanged");
        };
        assert_eq!(snapshot.len(), 76);
        assert_eq!(snapshot, expected);
    }

    #[test]
    fn complete_opus_pages_at_eof_allow_a_nonzero_initial_granule_offset() {
        let root = tempfile::tempdir().unwrap();
        let fixture = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tone.ogg"
        ))
        .unwrap();
        let mut reader = ogg::reading::PacketReader::new(Cursor::new(fixture));
        let file = File::create(root.path().join("finite.ogg")).unwrap();
        let mut writer = ogg::writing::PacketWriter::new(file);
        while let Some(packet) = reader.read_packet().unwrap() {
            let serial = packet.stream_serial();
            let granule =
                if packet.data.starts_with(b"OpusHead") || packet.data.starts_with(b"OpusTags") {
                    0
                } else {
                    packet.absgp_page() + 312
                };
            writer
                .write_packet(
                    packet.data,
                    serial,
                    ogg::writing::PacketWriteEndInfo::EndPage,
                    granule,
                )
                .unwrap();
        }
        drop(writer);
        let loaded = load(root.path(), "finite.ogg", 100000, 900.0).unwrap();
        assert_eq!(loaded.duration_secs, 0.5);
        let AudioData::Pcm16kMono(samples) = loaded.audio else {
            panic!("Expected decoded Opus")
        };
        assert_eq!(samples.len(), 8000);
    }
}
