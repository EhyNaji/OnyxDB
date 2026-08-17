use super::{
    BINLOG_RECORD_LENGTH_SIZE, BINLOG_RECORD_MAGIC, BinlogRecordIntegrity,
    CHECKSUMLESS_BINLOG_RECORD_MAGIC, DecodedBinlogRecord, MAX_BINLOG_RECORD_SIZE,
    MAX_SNAPSHOT_RECORD_SIZE, PersistenceError, PersistencePaths, apply_committed_batch,
    decode_binlog_record, decode_committed_batch, decode_snapshot_entry, encode_snapshot_entry,
    line_to_entry, read_u32_be,
};
use bytes::Bytes;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use onyxdb::engine::DataEntry;
use onyxdb::store::{ShardedStore, is_expired};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader as StdBufReader, BufWriter, Read, Write};
use std::path::Path;
use tracing::{info, warn};

pub(crate) const SNAPSHOT_MAGIC: &str = "ONYXSNAP";
pub(crate) const SNAPSHOT_VERSION: u8 = 2;
pub(crate) const MAX_SNAPSHOT_METADATA_SIZE: usize = 4096;
pub(crate) const MAX_SNAPSHOT_LINE_SIZE: usize = 512 * 1024 * 1024 + 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SnapshotFormat {
    Missing,
    Legacy,
    Versioned { watermark: u64 },
}

#[derive(Debug, Default)]
pub(crate) struct BinlogInspection {
    pub(crate) min_sequence: Option<u64>,
    pub(crate) max_sequence: u64,
    pub(crate) valid_len: u64,
    pub(crate) truncated_tail: bool,
    pub(crate) contains_checksumless_records: bool,
}

#[derive(Debug, Default)]
pub(crate) struct RecoveryState {
    pub(crate) last_sequence: u64,
    pub(crate) snapshot_watermark: u64,
}

fn read_bounded_utf8_line(
    reader: &mut impl BufRead,
    maximum_size: usize,
) -> Result<Option<String>, PersistenceError> {
    let mut bytes = Vec::new();
    let mut limited = reader.take((maximum_size + 1) as u64);
    if limited.read_until(b'\n', &mut bytes)? == 0 {
        return Ok(None);
    }
    if bytes.len() > maximum_size {
        return Err(PersistenceError::new(format!(
            "Snapshot line exceeds the {} byte limit",
            maximum_size
        )));
    }
    let mut line = String::from_utf8(bytes)
        .map_err(|_| PersistenceError::new("Snapshot contains invalid UTF-8"))?;
    while line.ends_with(['\r', '\n']) {
        line.pop();
    }
    Ok(Some(line))
}

pub(crate) fn inspect_snapshot(path: &Path) -> Result<SnapshotFormat, PersistenceError> {
    if !path.exists() {
        return Ok(SnapshotFormat::Missing);
    }
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let mut reader = StdBufReader::new(decoder);
    let first_line = read_bounded_utf8_line(&mut reader, MAX_SNAPSHOT_METADATA_SIZE)?
        .ok_or_else(|| PersistenceError::new("Snapshot is empty"))?;
    if !first_line.starts_with(SNAPSHOT_MAGIC) {
        return Ok(SnapshotFormat::Legacy);
    }

    let fields: Vec<&str> = first_line.split('\t').collect();
    if fields.len() != 3 || fields[0] != SNAPSHOT_MAGIC {
        return Err(PersistenceError::new("Malformed snapshot metadata header"));
    }
    let version = fields[1]
        .parse::<u8>()
        .map_err(|_| PersistenceError::new("Invalid snapshot format version"))?;
    if version != SNAPSHOT_VERSION {
        return Err(PersistenceError::new(format!(
            "Unsupported snapshot format version: {}",
            version
        )));
    }
    let watermark = fields[2]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid snapshot sequence watermark"))?;
    Ok(SnapshotFormat::Versioned { watermark })
}

pub(crate) fn for_each_binlog_record(
    path: &Path,
    mut visitor: impl FnMut(&[u8]) -> Result<(), PersistenceError>,
) -> Result<(u64, bool), PersistenceError> {
    if !path.exists() {
        return Ok((0, false));
    }

    let file = File::open(path)?;
    let mut reader = StdBufReader::new(file);
    let mut valid_len = 0u64;
    loop {
        let record_start = valid_len;
        let mut length_bytes = [0u8; 4];
        match reader.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                let file_len = fs::metadata(path)?.len();
                return Ok((record_start, file_len != record_start));
            }
            Err(error) => return Err(error.into()),
        }

        let record_len = u32::from_be_bytes(length_bytes) as usize;
        if record_len == 0 || record_len > MAX_BINLOG_RECORD_SIZE {
            return Err(PersistenceError::new(format!(
                "Invalid binlog record length: {}",
                record_len
            )));
        }
        let header_probe_length =
            record_len.min(BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE);
        let file_len = fs::metadata(path)?.len();
        let available_length = file_len.saturating_sub(record_start + 4);
        let readable_header_length = available_length.min(header_probe_length as u64) as usize;
        let mut header = [0u8; BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE];
        match reader.read_exact(&mut header[..readable_header_length]) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok((record_start, true));
            }
            Err(error) => return Err(error.into()),
        }
        let visible_header = &header[..readable_header_length];
        if readable_header_length < header_probe_length {
            if visible_header.starts_with(CHECKSUMLESS_BINLOG_RECORD_MAGIC) {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete checksumless ONX3 binlog record",
                ));
            }
            if readable_header_length >= BINLOG_RECORD_MAGIC.len()
                && !visible_header.starts_with(BINLOG_RECORD_MAGIC)
            {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete binlog record with unknown framing",
                ));
            }
            return Ok((record_start, true));
        }
        if visible_header.starts_with(BINLOG_RECORD_MAGIC)
            && record_len >= BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE
        {
            let mut offset = BINLOG_RECORD_MAGIC.len();
            let embedded_length = read_u32_be(visible_header, &mut offset)
                .expect("the fixed-size record header was read")
                as usize;
            if embedded_length != record_len {
                return Err(PersistenceError::new(format!(
                    "Binlog record length mismatch: outer length {}, embedded length {}",
                    record_len, embedded_length
                )));
            }
        }
        if available_length < record_len as u64 {
            if visible_header.starts_with(CHECKSUMLESS_BINLOG_RECORD_MAGIC) {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete checksumless ONX3 binlog record",
                ));
            }
            if !visible_header.starts_with(BINLOG_RECORD_MAGIC) {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete binlog record with unknown framing",
                ));
            }
            return Ok((record_start, true));
        }

        let mut record = vec![0u8; record_len];
        record[..header_probe_length].copy_from_slice(visible_header);
        match reader.read_exact(&mut record[header_probe_length..]) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok((record_start, true));
            }
            Err(error) => return Err(error.into()),
        }
        visitor(&record)?;
        valid_len = record_start + 4 + record_len as u64;
    }
}

pub(crate) fn inspect_binlog(path: &Path) -> Result<BinlogInspection, PersistenceError> {
    let mut inspection = BinlogInspection::default();
    let mut last_sequence: Option<u64> = None;
    let (valid_len, truncated_tail) = for_each_binlog_record(path, |record| {
        let DecodedBinlogRecord::Versioned {
            sequence,
            effects,
            integrity,
        } = decode_binlog_record(record)?;
        if let Some(previous) = last_sequence
            && previous.checked_add(1) != Some(sequence)
        {
            return Err(PersistenceError::new(format!(
                "Non-contiguous binlog sequence: {} after {}",
                sequence, previous
            )));
        }
        decode_committed_batch(effects).map_err(|error| {
            PersistenceError::new(format!(
                "Invalid committed-effect payload at binlog sequence {}: {}",
                sequence, error
            ))
        })?;
        inspection.contains_checksumless_records |=
            integrity == BinlogRecordIntegrity::ChecksumlessLegacy;
        inspection.min_sequence.get_or_insert(sequence);
        inspection.max_sequence = sequence;
        last_sequence = Some(sequence);
        Ok(())
    })?;
    inspection.valid_len = valid_len;
    inspection.truncated_tail = truncated_tail;
    Ok(inspection)
}

fn load_snapshot_entries(
    store: &ShardedStore,
    path: &Path,
    format: SnapshotFormat,
) -> Result<usize, PersistenceError> {
    if format == SnapshotFormat::Missing {
        return Ok(0);
    }
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let mut reader = StdBufReader::new(decoder);
    if matches!(format, SnapshotFormat::Versioned { .. }) {
        read_bounded_utf8_line(&mut reader, MAX_SNAPSHOT_METADATA_SIZE)?
            .ok_or_else(|| PersistenceError::new("Snapshot metadata header is missing"))?;

        let mut count = 0;
        loop {
            let mut length_bytes = [0u8; 4];
            if reader.read(&mut length_bytes[..1])? == 0 {
                break;
            }
            reader.read_exact(&mut length_bytes[1..])?;
            let record_length = u32::from_be_bytes(length_bytes) as usize;
            if record_length == 0 || record_length > MAX_SNAPSHOT_RECORD_SIZE {
                return Err(PersistenceError::new(format!(
                    "Invalid snapshot record length: {}",
                    record_length
                )));
            }
            let mut record = vec![0u8; record_length];
            reader.read_exact(&mut record)?;
            let (key, entry) = decode_snapshot_entry(&record)?;
            if !is_expired(&entry) {
                store.set_value(key, entry.value, entry.expires_at);
                count += 1;
            }
        }
        return Ok(count);
    }

    let mut count = 0;
    let mut skipped = 0;
    while let Some(line) = read_bounded_utf8_line(&mut reader, MAX_SNAPSHOT_LINE_SIZE)? {
        match line_to_entry(&line) {
            Some((key, entry)) if !is_expired(&entry) => {
                store.set_raw(key, entry);
                count += 1;
            }
            Some(_) => {}
            None => skipped += 1,
        }
    }
    if skipped > 0 {
        warn!(
            "Legacy snapshot: {} malformed entries were skipped",
            skipped
        );
    }
    Ok(count)
}

pub(crate) fn load_data_from_paths(
    store: &ShardedStore,
    paths: &PersistencePaths,
) -> Result<RecoveryState, PersistenceError> {
    recover_interrupted_binlog_rotation(paths)?;
    let snapshot_path = if paths.snapshot.exists() {
        &paths.snapshot
    } else if paths.snapshot_backup.exists() {
        warn!(
            "Primary snapshot is missing; recovering from {}",
            paths.snapshot_backup.display()
        );
        &paths.snapshot_backup
    } else {
        &paths.snapshot
    };
    let snapshot_format = inspect_snapshot(snapshot_path)?;
    if snapshot_format == SnapshotFormat::Legacy {
        return Err(PersistenceError::new(
            "Unsupported unsafe legacy snapshot format; create a verified versioned snapshot before upgrading",
        ));
    }
    let binlog = inspect_binlog(&paths.binlog)?;
    if binlog.contains_checksumless_records {
        warn!(
            "Recovery accepted structurally valid checksumless ONX3 records; compact the dataset to replace this legacy recovery history"
        );
    }
    let snapshot_watermark = match snapshot_format {
        SnapshotFormat::Versioned { watermark } => watermark,
        SnapshotFormat::Missing => 0,
        SnapshotFormat::Legacy => unreachable!(),
    };
    if let Some(first_sequence) = binlog.min_sequence
        && first_sequence > snapshot_watermark.saturating_add(1)
    {
        return Err(PersistenceError::new(
            "Binlog begins after the snapshot recovery boundary",
        ));
    }

    let staging = ShardedStore::new();
    let snapshot_count = load_snapshot_entries(&staging, snapshot_path, snapshot_format)?;
    info!("Snapshot loaded: {} active entries", snapshot_count);

    let mut replayed = 0usize;
    for_each_binlog_record(&paths.binlog, |record| {
        let DecodedBinlogRecord::Versioned {
            sequence, effects, ..
        } = decode_binlog_record(record)?;
        if sequence <= snapshot_watermark {
            return Ok(());
        }
        let batch = decode_committed_batch(effects)?;
        apply_committed_batch(&staging, &batch);
        replayed += 1;
        Ok(())
    })?;

    if binlog.truncated_tail {
        warn!(
            "Truncating incomplete binlog tail at byte {}",
            binlog.valid_len
        );
        let file = OpenOptions::new().write(true).open(&paths.binlog)?;
        file.set_len(binlog.valid_len)?;
        file.sync_all()?;
    }
    store.replace_all(staging.raw_entries());
    info!("Binlog replayed: {} commands", replayed);
    cleanup_redundant_binlog_rotation_files(paths);

    Ok(RecoveryState {
        last_sequence: snapshot_watermark.max(binlog.max_sequence),
        snapshot_watermark,
    })
}

fn recover_interrupted_binlog_rotation(paths: &PersistencePaths) -> Result<(), PersistenceError> {
    if paths.binlog.exists() {
        return Ok(());
    }
    if paths.binlog_backup.exists() {
        warn!(
            "Active binlog is missing; restoring interrupted rotation from {}",
            paths.binlog_backup.display()
        );
        durable_rename(&paths.binlog_backup, &paths.binlog)?;
        sync_parent_directory(&paths.binlog)?;
        return Ok(());
    }
    if paths.binlog_temp.exists() {
        return Err(PersistenceError::new(
            "Binlog rotation temporary file exists without an active or backup binlog",
        ));
    }
    Ok(())
}

fn cleanup_redundant_binlog_rotation_files(paths: &PersistencePaths) {
    let mut removed = false;
    for path in [&paths.binlog_temp, &paths.binlog_backup] {
        match fs::remove_file(path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(
                "Unable to remove redundant binlog rotation file {}: {}",
                path.display(),
                error
            ),
        }
    }
    if removed && let Err(error) = sync_parent_directory(&paths.binlog) {
        warn!(
            "Unable to synchronize redundant binlog rotation cleanup: {}",
            error
        );
    }
}

#[cfg(unix)]
pub(crate) fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(windows)]
pub(crate) fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    // Windows metadata durability is provided by durable_rename using
    // MOVEFILE_WRITE_THROUGH. FlushFileBuffers on a directory handle is not
    // consistently supported and returns access denied on common systems.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Directory synchronization is unsupported on this platform",
    ))
}

#[cfg(windows)]
pub(crate) fn durable_rename(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    #[link(name = "Kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }

    let existing: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let replacement: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    let result = unsafe {
        MoveFileExW(
            existing.as_ptr(),
            replacement.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
pub(crate) fn durable_rename(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::rename(from, to)
}

trait SnapshotInstaller {
    fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()>;
    fn sync_parent(&mut self, path: &Path) -> std::io::Result<()>;
}

struct OperatingSystemSnapshotInstaller;

impl SnapshotInstaller for OperatingSystemSnapshotInstaller {
    fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
        durable_rename(from, to)
    }

    fn sync_parent(&mut self, path: &Path) -> std::io::Result<()> {
        sync_parent_directory(path)
    }
}

pub(crate) fn write_snapshot_file(
    entries: Vec<(Bytes, DataEntry)>,
    watermark: u64,
    paths: &PersistencePaths,
) -> Result<(), PersistenceError> {
    write_snapshot_file_with_installer(
        entries,
        watermark,
        paths,
        &mut OperatingSystemSnapshotInstaller,
    )
}

fn write_snapshot_file_with_installer(
    entries: Vec<(Bytes, DataEntry)>,
    watermark: u64,
    paths: &PersistencePaths,
    installer: &mut impl SnapshotInstaller,
) -> Result<(), PersistenceError> {
    let file = File::create(&paths.snapshot_temp)?;
    let mut encoder = GzEncoder::new(BufWriter::new(file), Compression::default());
    writeln!(
        encoder,
        "{}\t{}\t{}",
        SNAPSHOT_MAGIC, SNAPSHOT_VERSION, watermark
    )?;
    for (key, entry) in entries {
        let record = encode_snapshot_entry(&key, &entry)?;
        let record_length = u32::try_from(record.len())
            .map_err(|_| PersistenceError::new("Snapshot entry exceeds the format limit"))?;
        encoder.write_all(&record_length.to_be_bytes())?;
        encoder.write_all(&record)?;
    }
    let mut writer = encoder.finish()?;
    writer.flush()?;
    let snapshot_file = writer
        .into_inner()
        .map_err(|error| PersistenceError::new(error.into_error().to_string()))?;
    snapshot_file.sync_all()?;
    drop(snapshot_file);

    if paths.snapshot.exists() {
        installer.rename(&paths.snapshot, &paths.snapshot_backup)?;
        installer.sync_parent(&paths.snapshot)?;
    }

    if let Err(error) = installer.rename(&paths.snapshot_temp, &paths.snapshot) {
        if !paths.snapshot.exists() && paths.snapshot_backup.exists() {
            let _ = installer.rename(&paths.snapshot_backup, &paths.snapshot);
            let _ = installer.sync_parent(&paths.snapshot);
        }
        return Err(error.into());
    }
    installer.sync_parent(&paths.snapshot)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{
        CommittedBatch, CommittedEffect, PersistentEntry, encode_committed_batch,
        encode_versioned_binlog_record,
    };
    use onyxdb::clock::unix_seconds;
    use onyxdb::engine::OnyxValue;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let id = NEXT_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "onyxdb-snapshot-install-{}-{}",
                std::process::id(),
                id
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn paths(&self) -> PersistencePaths {
            PersistencePaths::in_directory(&self.0)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Copy)]
    enum InstallFailureTiming {
        BeforeRename,
        AfterRename,
    }

    struct FailingSnapshotInstaller {
        install_target: PathBuf,
        timing: InstallFailureTiming,
        failed: bool,
    }

    impl SnapshotInstaller for FailingSnapshotInstaller {
        fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
            if to == self.install_target && !self.failed {
                self.failed = true;
                if matches!(self.timing, InstallFailureTiming::AfterRename) {
                    durable_rename(from, to)?;
                }
                return Err(std::io::Error::other("injected snapshot install failure"));
            }
            durable_rename(from, to)
        }

        fn sync_parent(&mut self, path: &Path) -> std::io::Result<()> {
            sync_parent_directory(path)
        }
    }

    fn snapshot_entries(value: &'static [u8]) -> Vec<(Bytes, DataEntry)> {
        let timestamp = unix_seconds();
        vec![(
            Bytes::from_static(b"key"),
            DataEntry {
                value: OnyxValue::Blob(Bytes::from_static(value)),
                expires_at: None,
                created_at: timestamp,
                last_accessed: timestamp,
            },
        )]
    }

    fn write_following_binlog_record(paths: &PersistencePaths) -> Vec<u8> {
        let batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"key"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"new")),
                expires_at: None,
            },
        }])
        .unwrap();
        let effect = encode_committed_batch(&batch).unwrap();
        let record = encode_versioned_binlog_record(2, &effect).unwrap();
        let mut framed = Vec::with_capacity(4 + record.len());
        framed.extend_from_slice(&(record.len() as u32).to_be_bytes());
        framed.extend_from_slice(&record);
        fs::write(&paths.binlog, &framed).unwrap();
        framed
    }

    fn assert_recovers_new_value(paths: &PersistencePaths) {
        let store = ShardedStore::new();
        let recovered = load_data_from_paths(&store, paths).unwrap();
        assert_eq!(recovered.last_sequence, 2);
        assert_eq!(
            store
                .get_entry(&Bytes::from_static(b"key"))
                .map(|entry| entry.value),
            Some(OnyxValue::Blob(Bytes::from_static(b"new")))
        );
    }

    #[test]
    fn install_failure_before_rename_restores_previous_snapshot_and_keeps_binlog() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        write_snapshot_file(snapshot_entries(b"old"), 1, &paths).unwrap();
        let binlog = write_following_binlog_record(&paths);
        let mut installer = FailingSnapshotInstaller {
            install_target: paths.snapshot.clone(),
            timing: InstallFailureTiming::BeforeRename,
            failed: false,
        };

        let error =
            write_snapshot_file_with_installer(snapshot_entries(b"new"), 2, &paths, &mut installer)
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected snapshot install failure")
        );
        assert_eq!(
            inspect_snapshot(&paths.snapshot).unwrap(),
            SnapshotFormat::Versioned { watermark: 1 }
        );
        assert_eq!(fs::read(&paths.binlog).unwrap(), binlog);
        assert_recovers_new_value(&paths);
    }

    #[test]
    fn ambiguous_install_error_keeps_new_snapshot_recoverable_without_discarding_history() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        write_snapshot_file(snapshot_entries(b"old"), 1, &paths).unwrap();
        let binlog = write_following_binlog_record(&paths);
        let mut installer = FailingSnapshotInstaller {
            install_target: paths.snapshot.clone(),
            timing: InstallFailureTiming::AfterRename,
            failed: false,
        };

        let error =
            write_snapshot_file_with_installer(snapshot_entries(b"new"), 2, &paths, &mut installer)
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected snapshot install failure")
        );
        assert_eq!(
            inspect_snapshot(&paths.snapshot).unwrap(),
            SnapshotFormat::Versioned { watermark: 2 }
        );
        assert_eq!(fs::read(&paths.binlog).unwrap(), binlog);
        assert_recovers_new_value(&paths);
    }

    #[test]
    fn interrupted_binlog_rotation_restores_the_full_backup() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        write_snapshot_file(snapshot_entries(b"old"), 1, &paths).unwrap();
        let suffix = write_following_binlog_record(&paths);
        durable_rename(&paths.binlog, &paths.binlog_backup).unwrap();
        fs::write(&paths.binlog_temp, &suffix).unwrap();
        assert!(!paths.binlog.exists());

        assert_recovers_new_value(&paths);

        assert!(paths.binlog.exists());
        assert!(!paths.binlog_backup.exists());
        assert!(!paths.binlog_temp.exists());
    }

    #[test]
    fn completed_binlog_rotation_prefers_the_active_suffix() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        write_snapshot_file(snapshot_entries(b"old"), 1, &paths).unwrap();
        let active_suffix = write_following_binlog_record(&paths);
        fs::write(&paths.binlog_backup, b"obsolete corrupt backup").unwrap();

        assert_recovers_new_value(&paths);

        assert_eq!(fs::read(&paths.binlog).unwrap(), active_suffix);
        assert!(!paths.binlog_backup.exists());
    }

    #[test]
    fn orphaned_binlog_rotation_temporary_file_is_rejected() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        fs::write(&paths.binlog_temp, b"uncommitted suffix").unwrap();

        let error = load_data_from_paths(&ShardedStore::new(), &paths)
            .unwrap_err()
            .to_string();

        assert!(error.contains("temporary file exists without an active or backup binlog"));
    }
}
