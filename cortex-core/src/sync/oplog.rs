//! Operation log — append-only JSONL files for sync.
//!
//! Each device writes to its own oplog files. Other devices read and replay them.
//! Files are rotated when they exceed `MAX_FILE_SIZE` or at date boundaries.

use crate::belief::Belief;
use crate::people::Person;
use crate::procedural::Pattern;
use crate::sync::hlc::HlcTimestamp;
use crate::types::*;
use crate::CortexError;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Maximum oplog file size before rotation (1 MB).
const MAX_FILE_SIZE: u64 = 1_048_576;

/// A single sync operation — one line in the JSONL file.
/// Device ID is embedded in `hlc.device_id` — no separate field needed.
/// HMAC protects operation integrity against tampering (computed without the hmac field itself).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncOp {
    pub op_id: Uuid,
    pub hlc: HlcTimestamp,
    pub payload: SyncPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hmac: Option<String>, // base64-encoded HMAC-SHA256 of (op_id, hlc, payload) without hmac field
}

/// Helper: SyncOp without HMAC field (for computing the HMAC).
#[derive(Debug, Serialize, Deserialize)]
struct SyncOpWithoutHmac {
    pub op_id: Uuid,
    pub hlc: HlcTimestamp,
    pub payload: SyncPayload,
}

/// The actual operation payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub enum SyncPayload {
    MemoryUpsert { memory: MemObject },
    MemoryDelete { id: Uuid },
    PersonUpsert { person: Person },
    PersonDelete { id: Uuid },
    BeliefUpsert { belief: Belief },
    PatternUpsert { pattern: Pattern },
    LinkUpsert {
        source_id: Uuid,
        target_id: Uuid,
        relation: crate::types::LinkRelation,
        strength: f32,
    },
}

/// Append-only JSONL writer for a device's oplog.
pub struct OpLogWriter {
    device_dir: PathBuf,
    current_file: Option<File>,
    current_path: PathBuf,
    current_date: String,
    current_seq: u32,
    crypto: Option<std::sync::Arc<crate::sync::crypto::CryptoContext>>,
}

impl OpLogWriter {
    pub fn new(
        device_dir: PathBuf,
        crypto: Option<std::sync::Arc<crate::sync::crypto::CryptoContext>>,
    ) -> Result<Self, CortexError> {
        fs::create_dir_all(&device_dir)
            .map_err(|e| CortexError::Storage(format!("Failed to create device dir: {}", e)))?;

        let mut writer = Self {
            device_dir,
            current_file: None,
            current_path: PathBuf::new(),
            current_date: String::new(),
            current_seq: 0,
            crypto,
        };
        writer.rotate_if_needed()?;
        Ok(writer)
    }

    /// Append a SyncOp to the current oplog file and flush.
    pub fn append(&mut self, op: SyncOp) -> Result<(), CortexError> {
        self.append_buffered(op)?;
        self.flush()
    }

    /// Append without flushing — use with `flush()` after a batch.
    pub fn append_buffered(&mut self, mut op: SyncOp) -> Result<(), CortexError> {
        use zeroize::Zeroize;
        self.rotate_if_needed()?;

        // Compute HMAC if we have crypto context (for integrity protection)
        if let Some(ref ctx) = self.crypto {
            let op_without_hmac = SyncOpWithoutHmac {
                op_id: op.op_id,
                hlc: op.hlc.clone(),
                payload: op.payload.clone(),
            };
            // Serialize without whitespace (canonical form) for deterministic HMAC
            let hmac_json = serde_json::to_string(&op_without_hmac)
                .map_err(|e| CortexError::Serialization(e.to_string()))?;

            // Compute HMAC-SHA256 of the canonical JSON (without the hmac field itself)
            let hmac_bytes = ctx.compute_operation_hmac(hmac_json.as_bytes());
            op.hmac = Some(base64::engine::general_purpose::STANDARD.encode(hmac_bytes));
        }

        let mut json = zeroize::Zeroizing::new(serde_json::to_string(&op)
            .map_err(|e| CortexError::Serialization(e.to_string()))?);

        let line = zeroize::Zeroizing::new(if let Some(ref ctx) = self.crypto {
            crate::sync::crypto::encrypt_line(ctx, json.as_bytes())?
        } else {
            json.to_string()
        });

        let file = self.current_file.as_mut()
            .ok_or_else(|| CortexError::Storage("No open oplog file".into()))?;
        if let Err(error) = writeln!(file, "{}", line.as_str()) {
            // A failed write can leave an arbitrary prefix. Never acknowledge a
            // later operation by appending it to that prefix, even without restart.
            self.current_file.take();
            return Err(CortexError::Storage(format!("Failed to write oplog: {error}")));
        }

        // Zeroize plaintext JSON from memory
        json.zeroize();

        Ok(())
    }

    /// Flush the oplog file to disk.
    pub fn flush(&mut self) -> Result<(), CortexError> {
        if let Some(ref mut file) = self.current_file {
            if let Err(error) = file.flush() {
                self.current_file.take();
                return Err(CortexError::Storage(format!("Failed to flush oplog: {error}")));
            }
        }
        Ok(())
    }

    fn rotate_if_needed(&mut self) -> Result<(), CortexError> {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        // Check if we need a new file (date change or size limit)
        let needs_new = if self.current_file.is_none() || today != self.current_date {
            true
        } else {
            let size = fs::metadata(&self.current_path).map(|m| m.len()).unwrap_or(0);
            size >= MAX_FILE_SIZE
        };

        if needs_new {
            if today != self.current_date {
                self.current_date = today.clone();
                self.current_seq = 1;
            } else {
                self.current_seq += 1;
            }

            // Find next available sequence number
            loop {
                let name = format!("oplog-{}-{:03}.jsonl", self.current_date, self.current_seq);
                let path = self.device_dir.join(&name);
                // Never append an acknowledged operation onto a crash-truncated
                // record. Preserve that file and rotate; readers retain its cursor
                // independently and continue with the next file.
                let appendable = match File::open(&path) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                    Err(e) => return Err(CortexError::Storage(format!("Failed to inspect oplog: {e}"))),
                    Ok(mut existing) => {
                        use std::io::{Read, Seek, SeekFrom};
                        let size = existing.metadata().map_err(|e| CortexError::Storage(e.to_string()))?.len();
                        if size == 0 { true } else if size >= MAX_FILE_SIZE { false } else {
                            existing.seek(SeekFrom::End(-1)).map_err(|e| CortexError::Storage(e.to_string()))?;
                            let mut last = [0u8;1];
                            existing.read_exact(&mut last).map_err(|e| CortexError::Storage(e.to_string()))?;
                            last[0] == b'\n'
                        }
                    }
                };
                if appendable {
                    self.current_path = path;
                    break;
                }
                self.current_seq += 1;
            }

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.current_path)
                .map_err(|e| CortexError::Storage(format!("Failed to open oplog: {}", e)))?;
            self.current_file = Some(file);
        }

        Ok(())
    }
}

/// Read operations from an oplog file, starting from a byte offset.
/// Returns (operations, new_byte_offset).
pub fn read_oplog(
    path: &Path,
    start_offset: u64,
    crypto: Option<&crate::sync::crypto::CryptoContext>,
) -> Result<(Vec<SyncOp>, u64), CortexError> {
    let file = File::open(path)
        .map_err(|e| CortexError::Storage(format!("Failed to open oplog {}: {}", path.display(), e)))?;

    let mut reader = BufReader::new(file);
    // Seek to start offset
    if start_offset > 0 {
        std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(start_offset))
            .map_err(|e| CortexError::Storage(format!("Failed to seek oplog: {}", e)))?;
    }

    let mut ops = Vec::new();
    let mut offset = start_offset;
    let mut consecutive_hmac_failures = 0;
    const CORRUPTION_THRESHOLD: u32 = 5; // Alert if 5+ consecutive HMAC failures

    loop {
        let mut buffer = zeroize::Zeroizing::new(Vec::new());
        let bytes_read = reader.read_until(b'\n', &mut buffer)
            .map_err(|e| CortexError::Storage(format!("Failed to read oplog line: {e}")))?;
        if bytes_read == 0 { break; }
        // Publication is checked in bytes: a crash may split a UTF-8 codepoint.
        // All incomplete tails retain their cursor before decoding/authentication.
        if !buffer.ends_with(b"\n") { break; }
        let line = match std::str::from_utf8(&buffer) {
            Ok(line) => line,
            Err(_) => {
                // A terminated malformed record follows the existing complete
                // corrupt-record policy; no bytes are authenticated or applied.
                tracing::warn!("Invalid UTF-8 in completed oplog record at offset {}", offset);
                offset += bytes_read as u64;
                continue;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            offset += bytes_read as u64;
            continue;
        }

        // Decrypt if needed, then deserialize
        let json_str = if crate::sync::crypto::is_encrypted_line(trimmed) {
            match crypto {
                Some(ctx) => {
                    match crate::sync::crypto::decrypt_line(ctx, trimmed) {
                        Ok(bytes) => match String::from_utf8(bytes) {
                            Ok(s) => s,
                            Err(_) => {
                                tracing::warn!("Invalid UTF-8 after decryption at offset {}", offset);
                                offset += bytes_read as u64;
                                continue;
                            }
                        },
                        Err(_) => {
                            tracing::warn!("Decryption failed at offset {}", offset);
                            offset += bytes_read as u64;
                            continue;
                        }
                    }
                }
                None => {
                    // Not corrupt, just unreadable *now*: stop here without advancing, so the
                    // line is read once a key is configured instead of being skipped forever.
                    // SyncEngine refuses to start key-less in an encrypted group, so reaching
                    // this means the file is stuck — say so loudly rather than stall silently.
                    tracing::error!(
                        path = %path.display(),
                        offset,
                        "Encrypted oplog line but no decryption key configured: this oplog is \
                         blocked at this offset until a sync passphrase is configured"
                    );
                    break;
                }
            }
        } else if crypto.is_some() {
            // SECURITY: encryption is enabled, but this line is not an encrypted envelope.
            // The writer always encrypts when a crypto context is present, so a plaintext
            // line here is never legitimate — it is corruption or an injection attempt by
            // someone with write access to the (untrusted) cloud sync directory. Accepting
            // it would bypass BOTH AES-GCM authentication and the operation HMAC (an injected
            // op simply omits the `hmac` field), so it must be rejected, not parsed.
            tracing::error!(
                "Rejecting unencrypted oplog line at offset {} while encryption is enabled — possible injection or downgrade attack",
                offset
            );
            offset += bytes_read as u64;
            continue;
        } else {
            trimmed.to_string()
        };

        let result = serde_json::from_str::<SyncOp>(&json_str);

        match result {
            Ok(op) => {
                // SECURITY: in encryption mode every legitimate op carries an HMAC (the writer
                // always attaches one when a crypto context is present). The HMAC key is derived
                // separately from the content-encryption key, so an attacker holding only a
                // leaked/old *content* key can forge a valid AES-GCM envelope but not the HMAC.
                // An encrypted op with `hmac: None` is therefore never legitimate — accepting it
                // would downgrade integrity to "content key only" and defeat forward secrecy after
                // key rotation. Reject it (fail closed).
                if op.hmac.is_none() && crypto.is_some() {
                    tracing::error!(
                        "Rejecting encrypted operation {} without HMAC at offset {} — possible forgery or downgrade attack",
                        op.op_id, offset
                    );
                    {
                        use zeroize::Zeroize;
                        let mut disposable = json_str;
                        disposable.zeroize();
                    }
                    offset += bytes_read as u64;
                    continue;
                }

                // Verify the HMAC. Encrypted ops without one were already rejected above; only
                // plaintext-mode ops (no crypto) may lack it.
                if let Some(hmac_str) = &op.hmac {
                    if let Some(ctx) = crypto {
                        // Reconstruct the operation without the HMAC field for verification
                        let op_without_hmac = SyncOpWithoutHmac {
                            op_id: op.op_id,
                            hlc: op.hlc.clone(),
                            payload: op.payload.clone(),
                        };
                        // Re-serialize in canonical form (same as write-time) for verification
                        if let Ok(hmac_json) = serde_json::to_string(&op_without_hmac) {
                            match ctx.verify_operation_hmac(hmac_json.as_bytes(), hmac_str) {
                                Ok(valid) => {
                                    if !valid {
                                        consecutive_hmac_failures += 1;
                                        tracing::error!("HMAC verification FAILED for operation {} at offset {} (failure #{}) — possible tampering or large-scale corruption", op.op_id, offset, consecutive_hmac_failures);

                                        // Check for sustained corruption pattern
                                        if consecutive_hmac_failures >= CORRUPTION_THRESHOLD {
                                            return Err(CortexError::Storage(format!(
                                                "OPLOG corruption detected: {} consecutive HMAC failures from offset {} onwards. Data integrity compromised. Consider restore from backup.",
                                                consecutive_hmac_failures, offset
                                            )));
                                        }

                                        // Zeroize plaintext JSON and skip this corrupted operation
                                        {
                                            use zeroize::Zeroize;
                                            let mut disposable = json_str;
                                            disposable.zeroize();
                                        }
                                        offset += bytes_read as u64;
                                        continue;  // Skip this operation, do not add to ops
                                    } else {
                                        consecutive_hmac_failures = 0;  // Reset on successful verification
                                    }
                                }
                                Err(e) => {
                                    consecutive_hmac_failures += 1;
                                    tracing::error!("HMAC verification error at offset {} (failure #{}) — unable to verify integrity: {}", offset, consecutive_hmac_failures, e);

                                    // Check for sustained corruption pattern
                                    if consecutive_hmac_failures >= CORRUPTION_THRESHOLD {
                                        return Err(CortexError::Storage(format!(
                                            "OPLOG corruption detected: {} consecutive verification errors from offset {} onwards. Data integrity compromised. Consider restore from backup.",
                                            consecutive_hmac_failures, offset
                                        )));
                                    }

                                    // Zeroize plaintext JSON and skip this operation
                                    {
                                        use zeroize::Zeroize;
                                        let mut disposable = json_str;
                                        disposable.zeroize();
                                    }
                                    offset += bytes_read as u64;
                                    continue;  // Skip this operation, do not add to ops
                                }
                            }
                        }
                    } else {
                        tracing::warn!("Encrypted operation with HMAC but no crypto context at offset {} — skipping", offset);
                        // Zeroize plaintext JSON and skip
                        {
                            use zeroize::Zeroize;
                            let mut disposable = json_str;
                            disposable.zeroize();
                        }
                        offset += bytes_read as u64;
                        continue;
                    }
                }

                // Zeroize plaintext JSON from memory
                {
                    use zeroize::Zeroize;
                    let mut disposable = json_str;
                    disposable.zeroize();
                }

                ops.push(op);
                offset += bytes_read as u64;
            }
            Err(_) => {
                // Only newline-terminated records reach this branch. Skip complete
                // corrupt records; an incomplete tail above retains its cursor.
                tracing::warn!("Skipping invalid oplog line at offset {}", offset);

                // Zeroize plaintext JSON from memory
                {
                    use zeroize::Zeroize;
                    let mut disposable = json_str;
                    disposable.zeroize();
                }

                offset += bytes_read as u64;
            }
        }
    }

    Ok((ops, offset))
}

/// List all oplog files for a given device directory, sorted by name (chronological).
pub fn list_oplog_files(device_dir: &Path) -> Result<Vec<PathBuf>, CortexError> {
    let mut files = Vec::new();
    if !device_dir.exists() {
        return Ok(files);
    }

    let entries = fs::read_dir(device_dir)
        .map_err(|e| CortexError::Storage(format!("Failed to read device dir: {}", e)))?;

    for entry in entries {
        let entry = entry
            .map_err(|e| CortexError::Storage(format!("Failed to read dir entry: {}", e)))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("oplog-") && name.ends_with(".jsonl") {
            files.push(entry.path());
        }
    }

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_op(device_id: &str, wall_ms: u64) -> SyncOp {
        SyncOp {
            op_id: Uuid::new_v4(),
            hlc: HlcTimestamp::new(wall_ms, 0, device_id),
            payload: SyncPayload::MemoryDelete { id: Uuid::new_v4() },
            hmac: None,}
    }

    /// Fast crypto context for tests (low KDF cost).
    fn test_crypto() -> crate::sync::crypto::CryptoContext {
        let mut m = crate::sync::crypto::new_encryption_manifest();
        m.kdf_params.time_cost = 1;
        m.kdf_params.mem_cost = 1024;
        crate::sync::crypto::derive_key("test-pass", &m).unwrap()
    }

    /// SECURITY: when encryption is enabled, an attacker with write access to the
    /// (untrusted) cloud sync directory must not be able to inject operations by
    /// writing an unencrypted, un-HMAC'd plaintext JSON line. Such lines bypass both
    /// AES-GCM authentication and the operation HMAC, so the reader must reject them.
    #[test]
    fn test_plaintext_injection_rejected_when_encryption_enabled() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("oplog-2026-01-01-001.jsonl");

        // Attacker forges a plaintext op (no encryption, no HMAC) and drops it into the dir.
        let forged = make_op("victim-device", 9999);
        let forged_id = forged.op_id;
        let line = serde_json::to_string(&forged).unwrap();
        assert!(!crate::sync::crypto::is_encrypted_line(&line));
        std::fs::write(&path, format!("{}\n", line)).unwrap();

        // The reader is operating in encryption mode (crypto context present).
        let ctx = test_crypto();
        let (ops, _offset) = read_oplog(&path, 0, Some(&ctx)).unwrap();

        assert!(
            ops.iter().all(|o| o.op_id != forged_id),
            "forged plaintext op must NOT be accepted when encryption is enabled"
        );
        assert!(ops.is_empty(), "no plaintext op may be replayed in encryption mode");
    }

    /// SECURITY: when encryption is enabled, an encrypted (AES-GCM) envelope whose inner
    /// op carries `hmac: None` must be rejected. The operation HMAC is keyed by a key that
    /// is derived separately from the content-encryption key, so an attacker who only holds
    /// a leaked/old *content* key can forge a valid AES-GCM envelope but cannot forge the
    /// HMAC. The legitimate writer ALWAYS attaches an HMAC when a crypto context is present,
    /// so an encrypted op with no HMAC is never legitimate — accepting it downgrades the
    /// integrity guarantee to "content key only" and defeats forward secrecy after rotation.
    #[test]
    fn test_encrypted_op_without_hmac_rejected() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("oplog-2026-01-01-001.jsonl");
        let ctx = test_crypto();

        // Attacker forges an op with no HMAC, then encrypts it with a (leaked) content key.
        let forged = make_op("victim-device", 9999);
        let forged_id = forged.op_id;
        assert!(forged.hmac.is_none());
        let plaintext = serde_json::to_string(&forged).unwrap();
        let enc_line = crate::sync::crypto::encrypt_line(&ctx, plaintext.as_bytes()).unwrap();
        assert!(crate::sync::crypto::is_encrypted_line(&enc_line));
        std::fs::write(&path, format!("{}\n", enc_line)).unwrap();

        let (ops, _offset) = read_oplog(&path, 0, Some(&ctx)).unwrap();

        assert!(
            ops.iter().all(|o| o.op_id != forged_id),
            "encrypted op with hmac:None must NOT be accepted when encryption is enabled"
        );
        assert!(ops.is_empty(), "no HMAC-less op may be replayed in encryption mode");
    }

    /// Legitimately encrypted lines must still be read back correctly in encryption mode.
    #[test]
    fn test_encrypted_lines_still_read_in_encryption_mode() {
        let tmp = TempDir::new().unwrap();
        let device_dir = tmp.path().join("device-a");
        let ctx = std::sync::Arc::new(test_crypto());

        let mut writer = OpLogWriter::new(device_dir.clone(), Some(ctx.clone())).unwrap();
        let op = make_op("device-a", 1000);
        let op_id = op.op_id;
        writer.append(op).unwrap();

        let files = list_oplog_files(&device_dir).unwrap();
        let (ops, _) = read_oplog(&files[0], 0, Some(&ctx)).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].op_id, op_id);
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let device_dir = tmp.path().join("device-a");

        let mut writer = OpLogWriter::new(device_dir.clone(), None).unwrap();
        let op1 = make_op("device-a", 1000);
        let op2 = make_op("device-a", 2000);
        let op1_id = op1.op_id;
        let op2_id = op2.op_id;
        writer.append(op1).unwrap();
        writer.append(op2).unwrap();

        let files = list_oplog_files(&device_dir).unwrap();
        assert_eq!(files.len(), 1);

        let (ops, offset) = read_oplog(&files[0], 0, None).unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].op_id, op1_id);
        assert_eq!(ops[1].op_id, op2_id);
        assert!(offset > 0);

        // Reading from the end offset should return nothing
        let (ops2, _) = read_oplog(&files[0], offset, None).unwrap();
        assert!(ops2.is_empty());
    }

    #[test]
    fn test_incremental_read() {
        let tmp = TempDir::new().unwrap();
        let device_dir = tmp.path().join("device-a");

        let mut writer = OpLogWriter::new(device_dir.clone(), None).unwrap();
        let op1 = make_op("device-a", 1000);
        writer.append(op1).unwrap();

        let files = list_oplog_files(&device_dir).unwrap();
        let (ops, offset1) = read_oplog(&files[0], 0, None).unwrap();
        assert_eq!(ops.len(), 1);

        // Write more ops
        let op2 = make_op("device-a", 2000);
        let op2_id = op2.op_id;
        writer.append(op2).unwrap();

        // Read from previous offset — should only get op2
        let (ops2, _) = read_oplog(&files[0], offset1, None).unwrap();
        assert_eq!(ops2.len(), 1);
        assert_eq!(ops2[0].op_id, op2_id);
    }

    #[test]
    fn test_serde_memory_upsert() {
        let mem = MemObject {
            id: Uuid::new_v4(),
            tier: MemoryTier::Episodic,
            content: MemContent::Text("hello world".into()),
            embedding: None,
            temporal: TemporalInfo::now(),
            source: MemSource::new("test"),
            salience: Salience::default(),
            privacy: PrivacyLevel::Private,
            links: Vec::new(),
            tags: Vec::new(),
            metadata: std::collections::HashMap::new(),
            content_hash: None,
            namespace: None,
        };

        let op = SyncOp {
            op_id: Uuid::new_v4(),
            hlc: HlcTimestamp::new(1000, 0, "device-a"),
            payload: SyncPayload::MemoryUpsert { memory: mem },
            hmac: None,};

        let json = serde_json::to_string(&op).unwrap();
        let parsed: SyncOp = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.op_id, op.op_id);
    }
}

#[cfg(all(test,unix))]
mod write_failure_tests {
    use super::*;
    use std::time::{Duration,Instant};
    #[test]
    fn limited_writer_child() {
        let Ok(root)=std::env::var("CORTEX_TEST_LIMITED_WRITER_ROOT") else { return; };
        let encrypted=std::env::var("CORTEX_TEST_LIMITED_WRITER_ENCRYPTED").unwrap()=="1";
        let crypto=encrypted.then(||std::sync::Arc::new(crate::sync::crypto::derive_key("test-only-io-failure",&crate::sync::crypto::new_encryption_manifest()).unwrap()));
        let root=PathBuf::from(root);
        let make=||SyncOp {op_id:Uuid::new_v4(),hlc:HlcTimestamp::new(1000,0,"io-writer"),payload:SyncPayload::MemoryDelete {id:Uuid::new_v4()},hmac:None};
        let mut writer=OpLogWriter::new(root.clone(),crypto.clone()).unwrap();
        let first=make();writer.append(first.clone()).unwrap();
        let old=writer.current_path.clone();let before=fs::metadata(&old).unwrap().len();
        let mut previous=std::mem::MaybeUninit::<libc::rlimit>::uninit();
        assert_eq!(unsafe {libc::getrlimit(libc::RLIMIT_FSIZE,previous.as_mut_ptr())},0);
        let previous=unsafe {previous.assume_init()};
        struct Restore {limit:libc::rlimit,handler:libc::sighandler_t}
        impl Drop for Restore { fn drop(&mut self) {assert_eq!(unsafe {libc::setrlimit(libc::RLIMIT_FSIZE,&self.limit)},0);unsafe {libc::signal(libc::SIGXFSZ,self.handler);}} }
        let restore=Restore {limit:previous,handler:unsafe {libc::signal(libc::SIGXFSZ,libc::SIG_IGN)}};
        let capped=libc::rlimit {rlim_cur:(before+32) as libc::rlim_t,rlim_max:previous.rlim_max};
        assert_eq!(unsafe {libc::setrlimit(libc::RLIMIT_FSIZE,&capped)},0);
        assert!(writer.append(make()).is_err(),"real file-size limit must interrupt append");
        let partial=fs::metadata(&old).unwrap().len();assert!(partial>before);assert_eq!(partial,before+32);
        drop(restore);
        let next=make();writer.append(next.clone()).unwrap();drop(writer);
        let mut ops=Vec::new();for file in list_oplog_files(&root).unwrap() {ops.extend(read_oplog(&file,0,crypto.as_deref()).unwrap().0);}
        assert_eq!(ops.len(),2,"acknowledged append after same-writer recovery must survive");assert_eq!(ops[0].op_id,first.op_id);assert_eq!(ops[1].op_id,next.op_id);
    }
    fn run_limited_writer(encrypted:bool) {
        let dir=tempfile::tempdir().unwrap();
        let mut child=std::process::Command::new(std::env::current_exe().unwrap()).args(["sync::oplog::write_failure_tests::limited_writer_child","--exact","--nocapture"]).env("CORTEX_TEST_LIMITED_WRITER_ROOT",dir.path()).env("CORTEX_TEST_LIMITED_WRITER_ENCRYPTED",if encrypted {"1"}else{"0"}).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
        let deadline=Instant::now()+Duration::from_secs(30);
        while child.try_wait().unwrap().is_none() { if Instant::now()>=deadline {let _=child.kill();let _=child.wait();panic!("limited writer child timed out");}std::thread::sleep(Duration::from_millis(10));}
        let output=child.wait_with_output().unwrap();assert!(output.status.success(),"limited child failed: {} {}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));
    }
    #[test]
    fn plaintext_partial_write_failure_recovers_same_writer() {run_limited_writer(false);}
    #[test]
    fn encrypted_partial_write_failure_recovers_same_writer() {run_limited_writer(true);}
}
