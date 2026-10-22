//! A disposable, scope-constrained projection of host-authoritative memory.
//! Never invokes ingest, plugins, inference, sync, or consolidation.
use crate::retrieval::{QueryBudget, RetrievalEngine, RetrievalQuery};
use crate::storage::{
    memory_index::{cosine_similarity, MemoryIndex},
    sqlite::SqliteStorage,
};
use crate::types::{MemContent, MemObject, MemObjectBuilder, MemSource, MemoryTier};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{params, params_from_iter, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ControlledError {
    #[error("invalid controlled input: {0}")]
    InvalidInput(&'static str),
    #[error("controlled state conflict: {0}")]
    Conflict(&'static str),
    #[error("replica is already open")]
    Busy,
    #[error("replica needs authoritative reconciliation")]
    NotReady,
    #[error("controlled store is closed")]
    Closed,
    #[error("controlled storage failure")]
    Storage,
}
impl ControlledError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::Conflict(_) => "conflict",
            Self::Busy => "busy",
            Self::NotReady => "not_ready",
            Self::Closed => "closed",
            Self::Storage => "storage",
        }
    }
}
impl From<rusqlite::Error> for ControlledError {
    fn from(_: rusqlite::Error) -> Self {
        Self::Storage
    }
}
impl From<crate::CortexError> for ControlledError {
    fn from(error: crate::CortexError) -> Self {
        match error {
            crate::CortexError::InvalidInput(_) => Self::InvalidInput("database mode"),
            _ => Self::Storage,
        }
    }
}
type Fence = (String, i64, Option<String>, bool, Option<String>);
type Result<T> = std::result::Result<T, ControlledError>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoreIdentity {
    pub owner_type: String,
    pub owner_id: String,
    pub bot_id: String,
    pub replica_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub kind: String,
    pub id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct SourceRef {
    pub kind: String,
    pub provider: String,
    pub id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub memory_id: String,
    pub revision: i64,
    pub scope: Scope,
    pub kind: String,
    pub text: String,
    pub source_refs: Vec<SourceRef>,
    #[serde(default)]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid_until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deletion {
    pub memory_id: String,
    pub revision: i64,
    pub scope: Scope,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Upsert { projection: Projection },
    Delete { deletion: Deletion },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub memory_id: String,
    pub revision: i64,
    pub state: String,
    pub outcome: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub generation: String,
    pub position: i64,
    pub durable: bool,
    pub outcomes: Vec<Outcome>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub generation: String,
    pub position: i64,
    pub sealed: bool,
    pub ready: bool,
    pub cleanup_pending: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct Hit {
    pub owner_type: String,
    pub owner_id: String,
    pub bot_id: String,
    #[serde(flatten)]
    pub projection: Projection,
    pub score: f32,
}
#[derive(Clone, Debug, Serialize)]
pub struct SearchResult {
    pub hits: Vec<Hit>,
    pub degraded: bool,
    pub truncated: bool,
}

/// Host-supplied bounds; no implicit embedding model is loaded.
#[derive(Clone, Debug)]
pub struct SearchOptions {
    pub limit: usize,
    pub embedding: Option<Vec<f32>>,
    pub max_candidates: usize,
    pub max_duration_ms: u64,
}
impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            limit: 10,
            embedding: None,
            max_candidates: 1000,
            max_duration_ms: 250,
        }
    }
}

struct State {
    storage: Option<SqliteStorage>,
    ready: bool,
    closed: bool,
    file: Option<File>,
}
impl State {
    fn storage(&self) -> Result<&SqliteStorage> {
        self.storage.as_ref().ok_or(ControlledError::Closed)
    }
}
/// Locks are dropped only after SQLite state and all calls have drained.
pub struct ControlledStore {
    state: Mutex<State>,
    identity: StoreIdentity,
    creator_pid: u32,
}

fn ident(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
        return Err(ControlledError::InvalidInput("identifier"));
    }
    Ok(())
}
fn json<T: Serialize>(x: &T) -> Result<String> {
    serde_json::to_string(x).map_err(|_| ControlledError::InvalidInput("serialization"))
}
fn digest<T: Serialize>(x: &T) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(json(x)?.as_bytes())))
}
fn decode<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|_| ControlledError::Storage)
}
fn scope_check(scope: &Scope, owner: &StoreIdentity) -> Result<()> {
    ident(&scope.id)?;
    if !["owner", "entity", "project", "workspace", "user"].contains(&scope.kind.as_str())
        || (scope.kind == "owner" && scope.id != owner.owner_id)
    {
        return Err(ControlledError::InvalidInput("scope"));
    }
    Ok(())
}
fn vector(v: &Option<Vec<f32>>) -> Result<()> {
    if let Some(v) = v {
        if v.is_empty()
            || v.len() > 4096
            || v.iter().any(|x| !x.is_finite())
            || !v.iter().map(|x| x * x).sum::<f32>().is_finite()
        {
            return Err(ControlledError::InvalidInput("embedding"));
        }
    }
    Ok(())
}
fn normalize(batch: &mut [Mutation], owner: &StoreIdentity) -> Result<()> {
    if batch.len() > 100 {
        return Err(ControlledError::InvalidInput("batch limit"));
    }
    let mut seen = HashSet::new();
    for m in batch {
        let (id, rev, scope) = match m {
            Mutation::Upsert { projection: p } => {
                if !["fact", "preference", "experience", "method"].contains(&p.kind.as_str())
                    || p.text.len() > 48000
                    || p.text.chars().count() > 12000
                {
                    return Err(ControlledError::InvalidInput("projection"));
                }
                if p.source_refs.is_empty() || p.source_refs.len() > 16 {
                    return Err(ControlledError::InvalidInput("sources"));
                }
                for r in &p.source_refs {
                    ident(&r.kind)?;
                    ident(&r.provider)?;
                    ident(&r.id)?;
                }
                p.source_refs.sort();
                p.source_refs.dedup();
                vector(&p.embedding)?;
                if let (Some(a), Some(b)) = (p.valid_from, p.valid_until) {
                    if a >= b {
                        return Err(ControlledError::InvalidInput("validity"));
                    }
                }
                (&p.memory_id, p.revision, &p.scope)
            }
            Mutation::Delete { deletion: d } => (&d.memory_id, d.revision, &d.scope),
        };
        ident(id)?;
        if rev <= 0 {
            return Err(ControlledError::InvalidInput("revision"));
        }
        scope_check(scope, owner)?;
        if !seen.insert(id.clone()) {
            return Err(ControlledError::InvalidInput("duplicate batch identity"));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn private_file(path: &Path) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| ControlledError::InvalidInput("unsafe store file"))?;
    let m = f.metadata().map_err(|_| ControlledError::Storage)?;
    if !m.is_file() || m.nlink() != 1 || m.uid() != unsafe { libc::geteuid() } {
        return Err(ControlledError::InvalidInput("unsafe store file"));
    }
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|_| ControlledError::Storage)?;
    Ok(f)
}
#[cfg(not(unix))]
fn private_file(_: &Path) -> Result<File> {
    Err(ControlledError::InvalidInput(
        "controlled storage currently requires Unix permissions",
    ))
}

fn effective(p: &Projection, now: DateTime<Utc>) -> bool {
    p.valid_from.is_none_or(|t| t <= now) && p.valid_until.is_none_or(|t| now < t)
}
impl Drop for ControlledStore {
    fn drop(&mut self) {
        if self.creator_pid != std::process::id() {
            // Never invoke copied SQLite state or a copied mutex in a fork child.
            // The OS reclaims this child's resources at process exit.
            let s = self.state.get_mut();
            if let Some(storage) = s.storage.take() {
                std::mem::forget(storage);
            }
        }
    }
}

impl ControlledStore {
    pub fn open(root: &Path, identity: StoreIdentity) -> Result<Self> {
        for x in [
            &identity.owner_type,
            &identity.owner_id,
            &identity.bot_id,
            &identity.replica_id,
        ] {
            ident(x)?;
        }
        if !root.is_absolute() || root.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(ControlledError::InvalidInput(
                "absolute private root required",
            ));
        }
        let mut prefix = PathBuf::new();
        for c in root.components() {
            prefix.push(c);
            if let Ok(m) = std::fs::symlink_metadata(&prefix) {
                if m.file_type().is_symlink() {
                    return Err(ControlledError::InvalidInput("symlink root"));
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::{MetadataExt, PermissionsExt};
                    let uid = unsafe { libc::geteuid() };
                    let mode = m.permissions().mode();
                    // An untrusted ancestor owner or writable non-sticky parent
                    // could replace the private root between validation and open.
                    if (m.uid() != 0 && m.uid() != uid) || (mode & 0o022 != 0 && mode & 0o1000 == 0)
                    {
                        return Err(ControlledError::InvalidInput("untrusted root ancestor"));
                    }
                }
            }
        }
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(root).map_err(|_| ControlledError::Storage)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            let m = std::fs::metadata(root).map_err(|_| ControlledError::Storage)?;
            if m.uid() != unsafe { libc::geteuid() } || m.permissions().mode() & 0o077 != 0 {
                return Err(ControlledError::InvalidInput("root must be owner-only"));
            }
        }
        let path = root.join(format!("{}.db", digest(&identity)?));
        let file = private_file(&path.with_extension("lock"))?;
        file.try_lock().map_err(|_| ControlledError::Busy)?;
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let p = PathBuf::from(format!("{}{suffix}", path.display()));
            if suffix.is_empty() || p.exists() || std::fs::symlink_metadata(&p).is_ok() {
                drop(private_file(&p)?);
            }
        }
        let storage = SqliteStorage::open_controlled(
            path.to_str()
                .ok_or(ControlledError::InvalidInput("path encoding"))?,
        )?;
        storage.with_controlled_conn(|conn| -> Result<()> {
            conn.execute_batch("CREATE TABLE IF NOT EXISTS controlled_meta(id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL, generation TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, sealed INTEGER NOT NULL DEFAULT 0, snapshot_position INTEGER, last_batch TEXT, last_receipt TEXT, dimension INTEGER, cleanup_pending INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS controlled_fences(memory_id TEXT PRIMARY KEY, scope TEXT NOT NULL, revision INTEGER NOT NULL, internal_id TEXT, live INTEGER NOT NULL, intent TEXT, valid_from INTEGER, valid_until INTEGER);
            CREATE INDEX IF NOT EXISTS controlled_scope ON controlled_fences(scope,live,internal_id);")?;
            conn.execute("INSERT OR IGNORE INTO controlled_meta(id,identity,generation) VALUES(1,?1,?2)",params![json(&identity)?,Uuid::new_v4().to_string()])?;
            let stored:String=conn.query_row("SELECT identity FROM controlled_meta WHERE id=1",[],|r|r.get(0))?;
            if decode::<StoreIdentity>(&stored)?!=identity { return Err(ControlledError::Conflict("store identity")); }
            let check:String=conn.query_row("PRAGMA quick_check",[],|r|r.get(0))?;
            if check!="ok" { return Err(ControlledError::Storage); }
            Ok(())
        })?;
        Ok(Self {
            state: Mutex::new(State {
                storage: Some(storage),
                ready: false,
                closed: false,
                file: Some(file),
            }),
            identity,
            creator_pid: std::process::id(),
        })
    }
    fn check_process(&self) -> Result<()> {
        if self.creator_pid != std::process::id() {
            return Err(ControlledError::Conflict(
                "inherited handle; open a new replica after fork",
            ));
        }
        Ok(())
    }
    fn status_locked(s: &State) -> Result<Status> {
        if s.closed {
            return Err(ControlledError::Closed);
        }
        s.storage()?.with_controlled_conn(|c| {
            Ok(c.query_row(
                "SELECT generation,position,sealed,cleanup_pending FROM controlled_meta WHERE id=1",
                [],
                |r| {
                    Ok(Status {
                        generation: r.get(0)?,
                        position: r.get(1)?,
                        sealed: r.get(2)?,
                        ready: s.ready,
                        cleanup_pending: r.get(3)?,
                    })
                },
            )?)
        })
    }
    pub fn status(&self) -> Result<Status> {
        self.check_process()?;
        let mut s = self.state.lock();
        let result = Self::status_locked(&s);
        Self::complete(&mut s, result)
    }
    fn complete<T>(s: &mut State, result: Result<T>) -> Result<T> {
        if matches!(result, Err(ControlledError::Storage)) {
            s.ready = false;
        }
        result
    }
    fn cleanup_locked(s: &State) -> Result<()> {
        s.storage()?.with_controlled_conn(|conn| -> Result<()> {
            let pending: bool = conn.query_row(
                "SELECT cleanup_pending FROM controlled_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            if pending && SqliteStorage::scrub_after_delete(conn) {
                // All calls are serialized by State; a newer delete cannot race
                // the checkpoint and have its pending flag cleared by this call.
                conn.execute(
                    "UPDATE controlled_meta SET cleanup_pending=0 WHERE id=1",
                    [],
                )?;
            }
            Ok(())
        })
    }
    pub fn retry_cleanup(&self) -> Result<Status> {
        self.check_process()?;
        let mut s = self.state.lock();
        let result = (|| {
            Self::cleanup_locked(&s)?;
            Self::status_locked(&s)
        })();
        Self::complete(&mut s, result)
    }
    pub fn close(&self) -> Result<()> {
        self.check_process()?;
        let mut s = self.state.lock();
        s.ready = false;
        s.closed = true;
        // The operation mutex drains executing calls. Queued calls see Closed;
        // no old Arc can retain SQLite or the OS lock after close returns.
        drop(s.storage.take());
        drop(s.file.take());
        Ok(())
    }
    pub fn reconcile(&self, generation: &str, position: i64) -> Result<Status> {
        self.check_process()?;
        ident(generation)?;
        if position < 0 {
            return Err(ControlledError::InvalidInput("position"));
        }
        let mut s = self.state.lock();
        let operation = (|| {
            s.ready = false;
            let v = Self::status_locked(&s)?;
            if !v.sealed {
                return Err(ControlledError::NotReady);
            }
            if position < 0 || v.generation != generation || v.position != position {
                return Err(ControlledError::Conflict(
                    "reconciliation position or generation",
                ));
            }
            s.ready = true;
            Self::status_locked(&s)
        })();
        Self::complete(&mut s, operation)
    }
    pub fn finish_snapshot(&self, position: i64) -> Result<Status> {
        self.check_process()?;
        if position < 0 {
            return Err(ControlledError::InvalidInput("position"));
        }
        let mut s = self.state.lock();
        let operation = (|| {
            let v = Self::status_locked(&s)?;
            if v.sealed {
                let original: i64 = s.storage()?.with_controlled_conn(|c| -> Result<i64> {
                    Ok(c.query_row(
                        "SELECT snapshot_position FROM controlled_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?)
                })?;
                if original == position {
                    return Ok(v);
                }
                return Err(ControlledError::Conflict("snapshot already sealed"));
            }
            let r = s.storage()?.with_controlled_conn(|c| -> Result<()> {
                c.execute(
                "UPDATE controlled_meta SET position=?1,snapshot_position=?1,sealed=1 WHERE id=1",
                [position],
            )?;
                Ok(())
            });
            if r.is_err() {
                s.ready = false;
            }
            r?;
            Self::status_locked(&s)
        })();
        Self::complete(&mut s, operation)
    }
    pub fn seed(&self, projections: Vec<Projection>) -> Result<Receipt> {
        self.mutate(
            projections
                .into_iter()
                .map(|projection| Mutation::Upsert { projection })
                .collect(),
            None,
        )
    }
    pub fn apply_batch(&self, mutations: Vec<Mutation>, position: i64) -> Result<Receipt> {
        if position < 0 {
            return Err(ControlledError::InvalidInput("position"));
        }
        self.mutate(mutations, Some(position))
    }
    pub fn upsert(&self, projection: Projection, position: i64) -> Result<Receipt> {
        self.apply_batch(vec![Mutation::Upsert { projection }], position)
    }
    pub fn delete(&self, deletion: Deletion, position: i64) -> Result<Receipt> {
        self.apply_batch(vec![Mutation::Delete { deletion }], position)
    }
    fn mutate(&self, mut batch: Vec<Mutation>, position: Option<i64>) -> Result<Receipt> {
        self.check_process()?;
        normalize(&mut batch, &self.identity)?;
        let batch_digest = digest(&batch)?;
        let mut s = self.state.lock();
        let operation = (|| {
            let before = Self::status_locked(&s)?;
            if position.is_none() && before.sealed {
                return Err(ControlledError::Conflict("snapshot is sealed"));
            }
            if position.is_some() && !before.sealed {
                return Err(ControlledError::NotReady);
            }
            let result=s.storage()?.with_controlled_conn(|conn| -> Result<Receipt> {
            let tx=conn.unchecked_transaction()?;
            if let Some(pos)=position {
                if pos==before.position {
                    let (d,r):(Option<String>,Option<String>)=tx.query_row("SELECT last_batch,last_receipt FROM controlled_meta WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
                    if d.as_deref()==Some(&batch_digest) { return decode(r.as_deref().ok_or(ControlledError::Storage)?); }
                    return Err(ControlledError::Conflict("position replay differs"));
                }
                if before.position.checked_add(1)!=Some(pos) { return Err(ControlledError::Conflict("nonsequential position")); }
            }
            let mut dimension:Option<usize>=tx.query_row("SELECT dimension FROM controlled_meta WHERE id=1",[],|r|r.get(0))?;
            let mut outcomes=Vec::new();
            for mutation in &batch {
                let (id,revision,scope,p)=match mutation { Mutation::Upsert{projection:p}=>(&p.memory_id,p.revision,&p.scope,Some(p)),Mutation::Delete{deletion:d}=>(&d.memory_id,d.revision,&d.scope,None) };
                let scope_json=json(scope)?;
                let old:Option<Fence>=tx.query_row("SELECT scope,revision,internal_id,live,intent FROM controlled_fences WHERE memory_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
                let intent=p.map(digest).transpose()?;
                let live=p.is_some();
                let mut outcome="applied";
                if let Some((old_scope,old_rev,_,old_live,old_intent))=&old {
                    if old_scope!=&scope_json { return Err(ControlledError::Conflict("immutable scope")); }
                    if revision<*old_rev { outcome="stale"; }
                    else if revision==*old_rev {
                        if *old_live!=live || old_intent!=&intent { return Err(ControlledError::Conflict("revision replay differs")); }
                        outcome="replayed";
                    }
                }
                if outcome=="applied" {
                    let internal=old.as_ref().and_then(|x|x.2.clone()).unwrap_or_else(||Uuid::new_v4().to_string());
                    tx.execute("DELETE FROM memories WHERE id=?1",[&internal])?;
                    if let Some(p)=p {
                        if let Some(v)=&p.embedding {
                            if dimension.is_some_and(|d|d!=v.len()) { return Err(ControlledError::InvalidInput("embedding dimension")); }dimension=Some(v.len());
                        }
                        let mut envelope=p.clone();envelope.embedding=None;
                        let mut mem=MemObjectBuilder::new(MemoryTier::Episodic,MemContent::Text(p.text.clone()),MemSource::new("bot_projection"))
                            .namespace(scope_json.clone()).meta("projection",serde_json::to_value(&envelope).map_err(|_|ControlledError::Storage)?).build();
                        mem.id=Uuid::parse_str(&internal).map_err(|_|ControlledError::Storage)?;
                        mem.embedding=p.embedding.clone().map(Arc::new);
                        SqliteStorage::insert_on_connection(&tx,&mem)?;
                    }
                    tx.execute("INSERT INTO controlled_fences(memory_id,scope,revision,internal_id,live,intent,valid_from,valid_until) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(memory_id) DO UPDATE SET revision=excluded.revision,internal_id=excluded.internal_id,live=excluded.live,intent=excluded.intent,valid_from=excluded.valid_from,valid_until=excluded.valid_until",params![id,scope_json,revision,internal,live,intent,p.and_then(|x|x.valid_from.map(|t|t.timestamp_millis())),p.and_then(|x|x.valid_until.map(|t|t.timestamp_millis()))])?;
                }
                #[cfg(test)]
                crash_barrier("inside_transaction");
                let (adopted_rev,adopted_live)=if outcome=="stale" { let x=old.as_ref().unwrap();(x.1,x.3) }else{(revision,live)};
                outcomes.push(Outcome{memory_id:id.clone(),revision:adopted_rev,state:if adopted_live{"live"}else{"withdrawn"}.into(),outcome:outcome.into()});
            }
            let receipt=Receipt{generation:before.generation.clone(),position:position.unwrap_or(before.position),durable:true,outcomes};
            tx.execute("UPDATE controlled_meta SET dimension=?1 WHERE id=1",[dimension])?;
            if let Some(pos)=position { tx.execute("UPDATE controlled_meta SET position=?1,last_batch=?2,last_receipt=?3 WHERE id=1",params![pos,batch_digest,json(&receipt)?])?; }
            if !batch.is_empty() { tx.execute("UPDATE controlled_meta SET cleanup_pending=1 WHERE id=1", [])?; }
            tx.commit()?;
            #[cfg(test)]
            crash_barrier("after_commit");
            Ok(receipt)
        });
            s.storage()?.invalidate_controlled_cache();
            if matches!(result, Err(ControlledError::Storage)) {
                s.ready = false;
            }
            if result.is_ok() {
                // The logical receipt already committed. Cleanup failure must never
                // turn it into an ambiguous failed mutation; expose pending status.
                if Self::cleanup_locked(&s).is_err() {
                    s.ready = false;
                }
            }
            result
        })();
        Self::complete(&mut s, operation)
    }
    pub fn search(
        &self,
        query: &str,
        scopes: &[Scope],
        options: SearchOptions,
    ) -> Result<SearchResult> {
        self.check_process()?;
        let SearchOptions {
            limit,
            embedding,
            max_candidates,
            max_duration_ms,
        } = options;
        if query.trim().is_empty()
            || query.chars().count() > 4000
            || !(1..=100).contains(&limit)
            || scopes.len() > 64
            || !(1..=10000).contains(&max_candidates)
            || !(1..=250).contains(&max_duration_ms)
        {
            return Err(ControlledError::InvalidInput("search bounds"));
        }
        vector(&embedding)?;
        for scope in scopes {
            scope_check(scope, &self.identity)?;
        }
        let started = Instant::now();
        let duration = Duration::from_millis(max_duration_ms);
        let deadline = started + duration;
        let Some(mut s) = self.state.try_lock_until(deadline) else {
            return Ok(SearchResult {
                hits: vec![],
                degraded: true,
                truncated: true,
            });
        };
        let operation = (|| {
            let status = Self::status_locked(&s)?;
            if !status.ready {
                return Err(ControlledError::NotReady);
            }
            let empty = || SearchResult {
                hits: vec![],
                degraded: false,
                truncated: false,
            };
            if scopes.is_empty() {
                return Ok(empty());
            }
            let mut scope_json: Vec<String> = scopes.iter().map(json).collect::<Result<_>>()?;
            scope_json.sort();
            scope_json.dedup();
            let terms: Vec<String> = query
                .split(|c: char| !c.is_alphanumeric())
                .filter(|s| !s.is_empty())
                .take(65)
                .map(|s| format!("\"{s}\""))
                .collect();
            if terms.len() > 64 {
                return Err(ControlledError::InvalidInput("query term limit"));
            }
            let fts = if terms.is_empty() {
                "\"\"".into()
            } else {
                terms.join(" AND ")
            };
            let fetched=s.storage()?.with_controlled_conn(|conn| -> Result<_> {
            let dim:Option<usize>=conn.query_row("SELECT dimension FROM controlled_meta WHERE id=1",[],|r|r.get(0))?;
            if let (Some(v),Some(d))=(&embedding,dim) { if v.len()!=d { return Err(ControlledError::InvalidInput("embedding dimension")); }}
            conn.progress_handler(1000,Some(move ||started.elapsed()>=duration));
            let run=(|| -> std::result::Result<Vec<(MemObject,bool)>,rusqlite::Error> {
                let placeholders=vec!["?";scope_json.len()].join(",");
                let sql=format!("SELECT m.id,m.tier,m.content_json,m.embedding_blob,m.temporal_json,m.source_json,m.salience_json,m.privacy_json,m.tags_json,m.metadata_json,m.links_json,m.content_hash,m.namespace, EXISTS(SELECT 1 FROM memories_fts WHERE rowid=m.rowid AND memories_fts MATCH ?) AS matched FROM memories m JOIN controlled_fences f ON f.internal_id=m.id WHERE f.live=1 AND f.scope IN ({placeholders}) AND (f.valid_from IS NULL OR f.valid_from<=?) AND (f.valid_until IS NULL OR f.valid_until>=?) ORDER BY matched DESC,m.id LIMIT ?");
                let now=Utc::now().timestamp_millis();
                let mut args:Vec<rusqlite::types::Value>=vec![fts.clone().into()];args.extend(scope_json.iter().cloned().map(Into::into));args.extend([now.into(),now.into(),((max_candidates+1)as i64).into()]);
                let mut stmt=conn.prepare(&sql)?;let rows=stmt.query_map(params_from_iter(args),|r|Ok((SqliteStorage::parse_mem_row(r)?,r.get(13)?)))?;rows.collect()
            })();
            conn.progress_handler(0,None::<fn()->bool>);
            match run { Ok(v)=>Ok(Some(v)),Err(rusqlite::Error::SqliteFailure(e,_)) if e.code==rusqlite::ErrorCode::OperationInterrupted => Ok(None),Err(_)=>Err(ControlledError::Storage) }
        })?;
            let Some(mut fetched) = fetched else {
                return Ok(SearchResult {
                    hits: vec![],
                    degraded: true,
                    truncated: true,
                });
            };
            let mut truncated = fetched.len() > max_candidates;
            fetched.truncate(max_candidates);
            // Share the existing scorer and cosine math, but never construct an HNSW
            // graph for a request-local candidate set. Each vector is bounded to 4096
            // dimensions and the deadline is checked between candidates.
            let index = MemoryIndex::new();
            let mut mems = HashMap::new();
            let mut lexical = HashMap::new();
            let mut scores = HashMap::new();
            for (mem, matched) in fetched {
                if Instant::now() >= deadline {
                    truncated = true;
                    break;
                }
                let envelope: Projection = serde_json::from_value(
                    mem.metadata
                        .get("projection")
                        .ok_or(ControlledError::Storage)?
                        .clone(),
                )
                .map_err(|_| ControlledError::Storage)?;
                if !effective(&envelope, Utc::now()) {
                    continue;
                }
                let score = match (&embedding, &mem.embedding) {
                    (Some(query), Some(vector)) => cosine_similarity(query, vector),
                    _ => 0.5,
                };
                if matched {
                    lexical.insert(mem.id, 1.0);
                }
                scores.insert(mem.id, score);
                mems.insert(mem.id, Arc::new(mem));
            }
            let mut q = RetrievalQuery::new(query, limit);
            q.embedding = embedding;
            q.budget = QueryBudget {
                max_candidates,
                max_duration: duration,
            };
            let ranked = RetrievalEngine::new(s.storage()?, &index).rank_candidates(
                &q,
                &scores,
                &mems,
                &lexical,
                Some(deadline),
            )?;
            let mut hits = Vec::new();
            for r in ranked {
                if Instant::now() >= deadline {
                    truncated = true;
                    break;
                }
                let projection: Projection = serde_json::from_value(
                    r.memory
                        .metadata
                        .get("projection")
                        .ok_or(ControlledError::Storage)?
                        .clone(),
                )
                .map_err(|_| ControlledError::Storage)?;
                if !effective(&projection, Utc::now()) {
                    continue;
                }
                hits.push(Hit {
                    owner_type: self.identity.owner_type.clone(),
                    owner_id: self.identity.owner_id.clone(),
                    bot_id: self.identity.bot_id.clone(),
                    projection,
                    score: r.score,
                });
            }
            Ok(SearchResult {
                hits,
                degraded: truncated || started.elapsed() >= duration,
                truncated,
            })
        })();
        Self::complete(&mut s, operation)
    }
}

#[cfg(test)]
fn crash_barrier(phase: &str) {
    if std::env::var("CORTEX_TEST_CRASH_PHASE").as_deref() == Ok(phase) {
        let marker = std::env::var("CORTEX_TEST_CRASH_MARKER").unwrap();
        std::fs::write(marker, phase).unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> StoreIdentity {
        StoreIdentity {
            owner_type: "merchant".into(),
            owner_id: "test-owner".into(),
            bot_id: "test-bot".into(),
            replica_id: "test-replica".into(),
        }
    }
    fn projection(id: &str) -> Projection {
        Projection {
            memory_id: id.into(),
            revision: 1,
            scope: Scope {
                kind: "owner".into(),
                id: "test-owner".into(),
            },
            kind: "preference".into(),
            text: "Remember the exact confirmed preference".into(),
            source_refs: vec![SourceRef {
                kind: "input".into(),
                provider: "bot".into(),
                id: "input-1".into(),
            }],
            valid_from: None,
            valid_until: None,
            embedding: None,
        }
    }
    #[test]
    fn controlled_writes_are_durable_and_do_not_infer_or_merge() {
        let root = tempfile::tempdir().unwrap();
        let store = ControlledStore::open(
            &root.path().canonicalize().unwrap().join("private"),
            identity(),
        )
        .unwrap();
        store.seed(vec![projection("a"), projection("b")]).unwrap();
        let state = store.state.lock();
        state
            .storage()
            .unwrap()
            .with_controlled_conn(|c| -> Result<()> {
                assert_eq!(
                    c.query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))?,
                    2
                );
                assert_eq!(
                    c.query_row("SELECT COUNT(*) FROM memories", [], |r| r.get::<_, i64>(0))?,
                    2
                );
                assert_eq!(
                    c.query_row("SELECT COUNT(*) FROM people", [], |r| r.get::<_, i64>(0))?,
                    0
                );
                assert_eq!(
                    c.query_row("SELECT COUNT(*) FROM beliefs", [], |r| r.get::<_, i64>(0))?,
                    0
                );
                Ok(())
            })
            .unwrap();
    }
    #[test]
    fn sql_failure_rolls_back_payload_fence_cursor_and_disables_queries() {
        let root = tempfile::tempdir().unwrap();
        let store = ControlledStore::open(
            &root.path().canonicalize().unwrap().join("private"),
            identity(),
        )
        .unwrap();
        store.seed(vec![projection("a")]).unwrap();
        store.finish_snapshot(10).unwrap();
        let status = store.status().unwrap();
        store.reconcile(&status.generation, 10).unwrap();
        store.state.lock().storage().unwrap().with_controlled_conn(|c| -> Result<()> {c.execute_batch("CREATE TRIGGER controlled_failure BEFORE INSERT ON memories BEGIN SELECT RAISE(ABORT,'injected failure'); END;")?;Ok(())}).unwrap();
        let error = store.upsert(projection("b"), 11).unwrap_err();
        assert_eq!(error.code(), "storage");
        assert!(!store.status().unwrap().ready);
        assert_eq!(store.status().unwrap().position, 10);
        store
            .state
            .lock()
            .storage()
            .unwrap()
            .with_controlled_conn(|c| -> Result<()> {
                assert_eq!(
                    c.query_row("SELECT COUNT(*) FROM controlled_fences", [], |r| r
                        .get::<_, i64>(0))?,
                    1
                );
                Ok(())
            })
            .unwrap();
    }
    #[test]
    fn native_close_releases_resources_before_old_handles_drop() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap().join("private");
        let old = ControlledStore::open(&path, identity()).unwrap();
        old.close().unwrap();
        let reopened = ControlledStore::open(&path, identity()).unwrap();
        assert_eq!(old.status().unwrap_err().code(), "closed");
        assert!(!reopened.status().unwrap().ready);
    }
    #[test]
    fn equal_revision_replay_with_changed_embedding_conflicts() {
        let root = tempfile::tempdir().unwrap();
        let store = ControlledStore::open(
            &root.path().canonicalize().unwrap().join("private"),
            identity(),
        )
        .unwrap();
        let mut p = projection("a");
        p.embedding = Some(vec![1.0, 0.0]);
        store.seed(vec![p.clone()]).unwrap();
        store.seed(vec![p.clone()]).unwrap();
        p.embedding = Some(vec![0.0, 1.0]);
        assert_eq!(store.seed(vec![p]).unwrap_err().code(), "conflict");
    }
    #[test]
    fn precise_validity_preserves_submillisecond_boundaries() {
        let instant: DateTime<Utc> = "2026-10-22T00:00:00.123456789Z".parse().unwrap();
        let mut p = projection("precision");
        p.valid_from = Some(instant);
        p.valid_until = Some(instant + chrono::Duration::nanoseconds(100));
        assert!(!effective(&p, instant - chrono::Duration::nanoseconds(1)));
        assert!(effective(&p, instant));
        assert!(effective(&p, instant + chrono::Duration::nanoseconds(99)));
        assert!(!effective(&p, instant + chrono::Duration::nanoseconds(100)));
    }
    #[test]
    fn read_storage_failure_disables_queries_until_reconciliation() {
        let root = tempfile::tempdir().unwrap();
        let store = ControlledStore::open(
            &root.path().canonicalize().unwrap().join("private"),
            identity(),
        )
        .unwrap();
        store.seed(vec![projection("a")]).unwrap();
        store.finish_snapshot(10).unwrap();
        let status = store.status().unwrap();
        store.reconcile(&status.generation, 10).unwrap();
        store
            .state
            .lock()
            .storage()
            .unwrap()
            .with_controlled_conn(|c| -> Result<()> {
                c.execute("UPDATE memories SET metadata_json='{}'", [])?;
                Ok(())
            })
            .unwrap();
        let error = store
            .search(
                "Remember",
                &[projection("a").scope],
                SearchOptions::default(),
            )
            .unwrap_err();
        assert_eq!(error.code(), "storage");
        assert!(!store.status().unwrap().ready);
    }
    #[test]
    fn crash_child() {
        let Ok(root) = std::env::var("CORTEX_TEST_CRASH_ROOT") else {
            return;
        };
        let store = ControlledStore::open(Path::new(&root), identity()).unwrap();
        let status = store.status().unwrap();
        store
            .reconcile(&status.generation, status.position)
            .unwrap();
        let mut p = projection("a");
        p.revision = 2;
        p.text = "committed revision two".into();
        store.upsert(p, 11).unwrap();
        panic!("crash barrier was not reached");
    }
    #[test]
    fn sigkill_inside_transaction_and_after_commit_preserves_atomic_replay() {
        use std::process::{Command, Stdio};
        struct ChildGuard(std::process::Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for (phase, expected_position) in [("inside_transaction", 10), ("after_commit", 11)] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap().join("private");
            let marker = dir.path().join("barrier");
            let store = ControlledStore::open(&root, identity()).unwrap();
            store.seed(vec![projection("a")]).unwrap();
            store.finish_snapshot(10).unwrap();
            store.close().unwrap();
            let mut child = ChildGuard(
                Command::new(std::env::current_exe().unwrap())
                    .args(["controlled::tests::crash_child", "--exact", "--nocapture"])
                    .env("CORTEX_TEST_CRASH_ROOT", &root)
                    .env("CORTEX_TEST_CRASH_PHASE", phase)
                    .env("CORTEX_TEST_CRASH_MARKER", &marker)
                    .stdout(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            let deadline = Instant::now() + Duration::from_secs(15);
            while !marker.exists() {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "child exited before barrier"
                );
                assert!(Instant::now() < deadline, "child did not reach barrier");
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(std::fs::read_to_string(&marker).unwrap(), phase);
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let reopened = ControlledStore::open(&root, identity()).unwrap();
            let status = reopened.status().unwrap();
            assert_eq!(status.position, expected_position);
            assert!(!status.ready);
            reopened.state.lock().storage().unwrap().with_controlled_conn(|c| -> Result<()> {
                let (revision, body): (i64, String) = c.query_row("SELECT f.revision,m.content_json FROM controlled_fences f JOIN memories m ON m.id=f.internal_id WHERE f.memory_id='a'", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
                assert_eq!(revision, if expected_position == 10 { 1 } else { 2 });
                assert_eq!(body.contains("committed revision two"), expected_position == 11);
                Ok(())
            }).unwrap();
            let mut p = projection("a");
            p.revision = 2;
            p.text = "committed revision two".into();
            let receipt = reopened.upsert(p.clone(), 11).unwrap();
            assert!(receipt.durable);
            assert_eq!(receipt.position, 11);
            assert_eq!(
                json(&receipt).unwrap(),
                json(&reopened.upsert(p, 11).unwrap()).unwrap()
            );
        }
    }
    #[test]
    fn cleanup_failure_remains_visible_and_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let store = ControlledStore::open(
            &dir.path().canonicalize().unwrap().join("private"),
            identity(),
        )
        .unwrap();
        store.seed(vec![projection("a")]).unwrap();
        store
            .state
            .lock()
            .storage()
            .unwrap()
            .with_controlled_conn(|c| -> Result<()> {
                c.execute_batch(
                    "UPDATE controlled_meta SET cleanup_pending=1; DROP TABLE memories_fts;",
                )?;
                Ok(())
            })
            .unwrap();
        assert!(store.retry_cleanup().unwrap().cleanup_pending);
        assert!(store.status().unwrap().cleanup_pending);
        store
            .state
            .lock()
            .storage()
            .unwrap()
            .with_controlled_conn(|c| -> Result<()> {
                c.execute_batch(
                    "CREATE VIRTUAL TABLE memories_fts USING fts5(id UNINDEXED, content);",
                )?;
                Ok(())
            })
            .unwrap();
        assert!(!store.retry_cleanup().unwrap().cleanup_pending);
    }
    #[cfg(unix)]
    #[test]
    fn untrusted_ancestor_swap_race_cannot_open_outside_root() {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let parent = base.join("unsafe");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o777)).unwrap();
        let target = base.join("outside");
        std::fs::create_dir(&target).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let swaps = Arc::new(AtomicUsize::new(0));
        let running = stop.clone();
        let count = swaps.clone();
        let attacker_parent = parent.clone();
        let attacker_target = target.clone();
        let attacker = std::thread::spawn(move || {
            let candidate = attacker_parent.join("candidate");
            while !running.load(Ordering::SeqCst) {
                std::fs::create_dir(&candidate).unwrap();
                let parked = attacker_parent.join("parked");
                std::fs::rename(&candidate, &parked).unwrap();
                std::os::unix::fs::symlink(&attacker_target, &candidate).unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                std::fs::remove_file(&candidate).unwrap();
                std::fs::remove_dir(&parked).unwrap();
            }
        });
        while swaps.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        let results: Vec<_> = (0..100)
            .map(|_| ControlledStore::open(&parent.join("candidate").join("private"), identity()))
            .collect();
        stop.store(true, Ordering::SeqCst);
        attacker.join().unwrap();
        assert!(results
            .iter()
            .all(|r| matches!(r, Err(ControlledError::InvalidInput(_)))));
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    }
}
