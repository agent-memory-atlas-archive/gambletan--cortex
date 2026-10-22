# Controlled memory API

Implementation contract for Cortex-side P0. Python distribution version: **2.5.0** (this branch; not yet published on PyPI). The installed `cortex_ai_memory` module exposes `ControlledStore` in addition to the unchanged personal `Cortex` class. The integration plan remains the scope/acceptance reference. Bot/Studio integration, PG authorization and candidate inference are outside this package.

## Install the reviewed build

The existing PyPI 2.4.0 release does not contain this API. Use the exact 2.5.0 wheel
from the reviewed local-CI artifact manifest for your platform, or build
from the reviewed source commit. The local verification matrix targets Python 3.12 on Linux x86_64 and macOS
arm64. GitHub Actions CI is disabled under the local-only CI policy. The Python release profile retains native symbols to avoid the reproduced macOS stripped-LINKEDIT load failure. Wheel files are platform/interpreter specific; do not install an incompatible
artifact or assume other targets have been verified.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install 'maturin>=1.0,<2.0'
.venv/bin/maturin build --release -m cortex-python/Cargo.toml --out dist --interpreter .venv/bin/python
.venv/bin/python -m pip install --force-reinstall dist/cortex_ai_memory-2.5.0-*.whl
.venv/bin/python -c 'from cortex_ai_memory import ControlledStore, ControlledError'
```

Do not use `pip install cortex-ai-memory` as proof this API is available until 2.5.0
has actually been published. Use a clean wheel output directory and retain the source
SHA and artifact checksum with the host's existing dependency manifest.

## Store and lifecycle

`ControlledStore(root, owner_type, owner_id, bot_id, replica_id)` opens a dedicated owner/bot/replica database under an absolute, private root. Paths inside that root are generated from the identity. The root must not be a symlink or contain `..`; resolve platform temporary-directory aliases before passing it. Existing ancestors must be owned by the service user or root and must not be group/world-writable unless sticky, preventing an untrusted local account from replacing the validated root. New root components are created individually and checked without following symlinks, including a name precreated under a shared sticky parent. The root itself is always owner-only. One process/handle may own a replica at a time. Handles inherited through `fork` raise `conflict` before touching copied locks/SQLite. After any Cortex store initialization, children must exec/spawn before opening stores; a pure-fork child cannot initialize another store. Use a separate replica for each spawned worker. Process-owned locks allow the parent to close and reopen its replica while an unused fork child remains alive. Different replicas have separate generation IDs and cursors. A personal database is never imported. A SQLite write transaction atomically claims personal or controlled mode before initialization; the persistent marker survives an initialization failure. Current legacy open/export paths refuse controlled databases. Older binaries must not be given these files.

- `status()` returns `{generation, position, sealed, ready, cleanup_pending}`. Every reopen starts not ready.
- `seed(projections)` atomically loads a batch of current authoritative memories while the snapshot is unsealed. Retrying identical revisions is safe. No seed after snapshot completion.
- `finish_snapshot(position)` durably seals a new snapshot at the PG committed snapshot watermark; does not enable queries. Repeating the same watermark succeeds; changing it after sealing conflicts.
- `reconcile(generation, position)` enables queries only after both exactly match local durable status and snapshot is sealed. An unsealed snapshot raises `not_ready`; a generation/position mismatch raises `conflict` and queries remain disabled. The caller must validate the source snapshot, reconcile retained changes or rebuild a new replica before this call; equality alone does not authorize data.
- `apply_batch(mutations, position)` durably and atomically applies the next sequential replay position. Empty batches can advance a position (e.g. an irrelevant event). Equal latest position is accepted only for the identical normalized batch and returns its original receipt. Older/gapped positions conflict without modifying data. Upsert/delete convenience methods accept one projection/deletion and the same position.
- `retry_cleanup()` retries physical SQLite/FTS/WAL cleanup and returns current status. Logical mutation receipts stay immutable. Every payload replacement/withdrawal atomically sets `cleanup_pending`; it remains true while a reader blocks WAL truncation or cleanup fails. A successful serialized retry clears it. Retry with bounded host backoff; backups and external copies require host retention controls.
- Last-reference destruction releases the GIL for native teardown. Explicit `close()` remains the deterministic lifecycle boundary, including on exception paths.
- `close()` refuses subsequent calls and drains current native operations before releasing the store lock. A new store object can reopen/reconcile the replica. Use a new replica ID for a fresh rebuild, reconcile it, atomically route new calls to it, drain/close the previous object, then retire old files under retention policy. No unsafe in-place file swap API is exposed.

Each projection has `memory_id` (opaque external ID), positive integer `revision`, `scope: {kind,id}`, `kind` (`fact`, `preference`, `experience`, `method`), `text`, `source_refs: [{kind,provider,id}]`, optional RFC3339 `valid_from`/`valid_until`, and optional numeric `embedding`. Missing validity bounds are unbounded. Validity is `[valid_from, valid_until)`. Scope kind is `owner`, `entity`, `project`, `workspace`, or `user`; owner scope ID must equal the store owner ID. Scope never changes for an existing memory ID, including after deletion. To change it, withdraw that identity and create another. Projection state is implicitly active; no inferred/candidate memory is accepted automatically.

A deletion is `{memory_id, revision, scope}` and retains an immutable-scope tombstone without text. Even unseen deletions require canonical scope. Lower memory revisions return a stale outcome; equal revision with changed semantic content conflicts; identical revision/content replays. A higher same-scope revision can restore a memory only when the caller has confirmed reactivation in PG. Different external IDs with identical text never merge.

Mutations are `{"op":"upsert","projection":...}` or `{"op":"delete","deletion":...}`. A receipt is `{generation, position, durable:true, outcomes:[{memory_id,revision,state,outcome}]}`; state is `live`/`withdrawn`, outcome is `applied`/`replayed`/`stale`. Receipts contain no memory text. The local payload, revision fence, receipt and replay position commit in one SQLite WAL transaction with synchronous FULL. Only then may the caller acknowledge PG delivery. After restore, compare local status against the remote cursor and reconcile/rebuild; never trust the remote cursor alone.

## Retrieval

`search(query, allowed_scopes, *, limit=10, embedding=None, max_candidates=1000, max_duration_ms=250)` returns `{hits, degraded, truncated}`. Hits include owner_type/owner_id/bot_id, memory_id, revision, scope, kind, text, source_refs, validity bounds and score. Native methods release the Python GIL. Call them through bounded host worker capacity, not unlimited async tasks. Cancellation cannot roll back an already committed mutation; retry the original position/batch.

`allowed_scopes` is required, supplied by the trusted host, and an empty list returns no hits. Candidates are filtered in SQL for scopes and coarse millisecond validity bounds, then exact RFC3339 timestamp precision is checked before ranking and again before returning hits; disabled graph/unscoped expansion cannot contribute. The existing FTS, cosine-similarity and ranking machinery are reused. FTS contributes a match signal without global BM25 statistics; only bounded authorized candidates are scored, so excluded records do not influence ranking. No request-local HNSW graph is built. The soft deadline covers lock acquisition, SQL work, vector scoring, ranking and hit projection, with cooperative checks between bounded work units; it is not a hard real-time OS scheduling SLA. Work is bounded by candidate/time limits; exhaustion reports degradation, never proof that no memory exists. Semantic lookup uses explicitly supplied vectors; no model download/inference occurs. Cortex enforces one vector dimension per store; the host must also keep the embedding model consistent. Model changes require rebuilding with new vectors.

Every hit is an **internal candidate**. Before any UI/log/context exposure, the Bot must reload current PG state, validate identity/scope/source access, active/withdrawn state, exact revision and both validity bounds using server time, and emit current PG text. Resolve required defaults by exact key from PG independently of Cortex hits/readiness. Recheck validity and withdrawal at model/action boundaries. Never inject Cortex-generated prompts alongside ContextProjector. If lookup is unavailable or incomplete use the existing bounded PG fallback; authority lookup failures fail closed.

## Limits and errors

Identifiers: 1–256 UTF-8 bytes, no control characters. Owner/scope kinds are constrained as above. Text: at most 12,000 characters / 48,000 bytes. Sources: 1–16; each field uses identifier limits. Batches: at most 100 entries. Query: 1–4,000 characters and at most 64 alphanumeric terms; limit 1–100; at most 64 allowed scopes; candidates 1–10,000; time budget 1–250 ms; embeddings 1–4,096 finite floats with finite squared norm, with a consistent store-wide dimension. Python inputs must use exact built-in dict/list/string/int/float/bool/None types; custom objects and container subclasses are rejected. Dictionary positions (including nested scopes, source references and batch envelopes) reject positional lists. Conversion checks container limits before copying, rejects nesting deeper than eight levels, and enforces a conservative 8 MiB JSON-size budget incrementally. Search options are keyword-only; malformed values (including bool or overflowing bounds) raise `invalid_input`. Duplicate memory IDs inside one batch are rejected. Unknown fields, bool/fractional/negative revisions or positions, invalid intervals and malformed vectors fail validation; source order is normalized for replay identity.

`ControlledError` has stable `.code`: `invalid_input`, `conflict`, `busy`, `not_ready`, `closed`, or `storage`. Stale memory revisions are receipt outcomes, not exceptions. Storage/commit ambiguity disables queries until reopen/reconciliation. No error text contains projection bodies. Python integer positions/revisions preserve precision up to SQLite's signed 64-bit maximum.

## Example (after installation)

```python
from cortex_ai_memory import ControlledStore

store = ControlledStore('/srv/private/cortex', 'merchant', 'm1', 'b1', 'worker1')
scope = {'kind': 'owner', 'id': 'm1'}
projection = {
    'memory_id': 'lang', 'revision': 1, 'scope': scope,
    'kind': 'preference', 'text': 'Reports use English',
    'source_refs': [{'kind': 'input', 'provider': 'bot', 'id': 'input1'}],
}
store.seed([projection])
store.finish_snapshot(100)  # real PG snapshot watermark, not a guessed event number
state = store.status()
store.reconcile(state['generation'], 100)  # only after authoritative reconciliation
hits = store.search('Reports', [scope])
# Validate each candidate against current PG permissions/state before use.
store.delete({'memory_id': 'lang', 'revision': 2, 'scope': scope}, 101)
store.close()
```

The current wheel uses plaintext SQLite; private filesystem controls are mandatory. No automatic sync/export/inference/consolidation is enabled. Logical withdrawal does not promise immediate erasure from backups or already-sent model requests. See the [integration plan](design/bot-memory-integration.md) for host-owned replay, rebuild, rollout and real-PG acceptance.

On Unix, filesystem SQLite URI names (`file:...`) are rejected by the private-file guard; use a filesystem path or `:memory:`. Personal database parents must also have trusted ancestry (service/root ownership; no group/world write unless sticky). The immediate database directory must not be group/world-writable, including sticky shared directories; create a service-owned private child directory instead. Trusted directory aliases are resolved and revalidated; final symlinks, hardlinks, foreign-owned files and non-regular files are rejected. Permissions are prepared without opening/closing existing database or sidecar descriptors, preserving active SQLite process locks.

The existing personal API constructor is `Cortex(db_path)`; it raises `RuntimeError` for a controlled database. It has no explicit close method; release its references when no longer used.


Personal `Cortex` retains its existing logical deletion semantics. Rust `Cortex`/`SqliteStorage` and Python `Cortex` now expose `cleanup_pending()` and `retry_cleanup()` (both return bool; true means physical work remains). Legacy deletion records pending cleanup in the same transaction as row removal, then invalidates caches/index/events regardless of cleanup contention. A persisted deletion token fences cleanup completion so an older retry cannot clear a newer deletion from another handle or process. Logical deletion success does not assert that WAL truncation succeeded; check status and retry after readers release their snapshots.

Existing databases without the erasure maintenance version undergo a checked, exclusive SQLite migration: rebuild current FTS data, VACUUM historical free pages, truncate WAL, then record completion. Upgrade may wait for other readers/writers and take longer for large files; quiesce other users and reopen if interrupted. Version zero remains pending through errors/crashes and is never reported as complete. New empty databases skip historical cleanup. This covers local SQLite artifacts, not backups, filesystems' old block copies, exports or content already sent elsewhere.
