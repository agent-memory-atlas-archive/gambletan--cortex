# Cortex integration with Bot memory: implementation plan

Date: 2026-10-22. Status: Cortex controlled core and Python binding implemented on this branch; host integration, package publication and deployment are not completed.

## 1. Goal and fixed boundaries

Reuse Cortex for retrieval of confirmed Bot memories before building another retrieval engine. Start with a disposable index; add inference and consolidation only after the controlled retrieval contract passes acceptance.

The upstream Bot contract is [kol-agent PR #24](https://github.com/HEchooo/kol-agent/pull/24), final head `eb52ceafdc854be832183169ee428280f7278cce`, especially its [memory, context, and acceptance design](https://github.com/HEchooo/kol-agent/blob/eb52ceafdc854be832183169ee428280f7278cce/docs/bot-workbench-memory-session-design.md). This plan refines its PostgreSQL-first retrieval direction: PostgreSQL remains authoritative, Cortex is an optional derived retrieval backend. Adoption must be recorded in the Bot design/roadmap by its owner before enabling the integration. It does not require embedding-based retrieval to ship the Bot.

- Bot MemoryManager/PostgreSQL owns identity, owner/bot/scope permissions, explicit versus candidate status, source validity, revisions, source ordering, expiry, correction, withdrawal, and public acknowledgements.
- Cortex owns derived storage, indexing, bounded ranking, and later candidate suggestions. It cannot acknowledge a successful business memory write or grant access.
- ContextProjector remains the sole model-context composer. Raw Cortex `get_context()` output is not injected alongside it.
- Workflow retains runs, checkpoints, approvals, actions, budget barriers, and recovery. Cortex receives no wallet, publishing, or execution authority.
- A local revision is an object concurrency token, not a cross-repository version/hash admission rule. No shared ORM or writes to another service's database.
- Existing personal Cortex, MCP, HTTP, sync, gateway, and cloud API signatures remain compatible. Private-file enforcement now rejects unsupported SQLite URI names and unsafe filesystem aliases as described in the API contract. The new integration uses a separate controlled store and explicit opt-in; personal archives and Muse exports are never imported implicitly.

## 2. Evidence and reuse at the planning baseline

Cortex baseline: `00f0e38da36bdafc8b40cf235aaac9e53bdf8548` (origin/main when planning began). Recheck functions against current main before implementing. Source links below are repository-relative; names, not historical line numbers, identify implementation seams.

| Existing code | Reuse | Gap for Bot integration |
| --- | --- | --- |
| [core ingest lifecycle](../../cortex-core/src/lib.rs), `ingest_with_options`, `prepare_ingest`, `finalize_ingest` | Existing storage/index machinery and input checks | Normal ingest performs inference, deduplication, derived preference writes, and periodic consolidation; disabling the interval alone is insufficient |
| [MemObject and MemSource](../../cortex-core/src/types.rs) | Stable internal UUID, namespace, metadata, source, and privacy | Add a validated external projection envelope; generic metadata alone does not enforce revision or scope invariants |
| [retrieval](../../cortex-core/src/retrieval.rs) | Existing ranking, lexical/vector retrieval, and QueryBudget | Controlled requests require authorized scope filtering throughout retrieval, structured results, and explicit degraded/truncated reporting |
| [storage](../../cortex-core/src/storage/sqlite.rs) and [storage trait](../../cortex-core/src/storage/traits.rs) | SQLite transactions, CRUD, FTS triggers, memory cache | Atomic projection payload/state changes, stale-write fencing, and controlled-store lifecycle |
| [core deletion](../../cortex-core/src/lib.rs), `delete_memory` | SQL removal, index removal, cache generation, deletion event | External identity/tombstone handling and crash/race acceptance; a normal delete alone does not reject a later stale projection |
| [Python binding](../../cortex-python/src/lib.rs) | Existing PyO3 package and native engine | Current API lacks namespace arguments and delete exposure; retrieve returns only ID/score/text, without verification metadata |
| [HTTP guard](../../cortex-http/src/main.rs), local server entrypoint | Preserve existing local-only use | The local HTTP API has no authentication and can read Private memories; it is not a tenant service |

The Python README example currently uses `cortex_python.PyCortex`, while the module/class declarations are `cortex_ai_memory.Cortex`. Verify the installed wheel and repair examples as part of the binding package work; do not infer installability from a source import.

An open wheel-build PR (#14 at planning time) owns macOS deployment-target fixes. Coordinate with that work instead of duplicating or overwriting its changes. This plan makes no claims about current published wheel availability.

The original owner-only database and plaintext-deletion fixes (`c1d8658`, `5f522d6`) have been reused as original cherry-picked commits in this branch, with conflicts resolved to preserve current main. Their tests and full diff are included in implementation review; they were not shipped main capabilities at the planning baseline.

## 3. First delivery and responsibilities

P0 indexes only confirmed, active, authorized memory supplied by MemoryManager. No raw transcript import, automatic fact/preference activation, people-identity merging, memory tidy application, cross-device sync, background inference, public API, or new UI is included.

Use the existing Python binding as the first integration surface. Extend it with a controlled-store object, leaving existing `Cortex` calls unchanged. Use Rust core and existing storage/retrieval helpers underneath; do not replicate ranking or expose direct SQLite mutation to Python. HTTP and MCP expansion are not P0 prerequisites. If supported wheel/platform packaging cannot satisfy the host, stop enablement and document that concrete gap; do not silently switch transport or expose cortex-http.

| Owner | Deliverables |
| --- | --- |
| Cortex | Controlled core API, persisted projection fences, scope-safe structured search, deletion/rebuild lifecycle, Python binding, distribution tests and documentation |
| chatbot-agent | PostgreSQL memory/outbox authority; adapter; replica cursors/reconciliation; permission/version revalidation; ContextProjector integration; fallback and end-to-end tests |
| Studio | Install the reviewed chatbot-agent/vendor package using the existing release process; retain current identity and permissions; no direct Cortex browser calls |
| Bot design owner | Record the optional retrieval backend and precise acceptance mapping in the existing design/roadmap |

The implemented Cortex/Python surface is specified in the [controlled memory API](../controlled-memory-api.md). Host PostgreSQL delivery and authorization behavior below remains the integration contract; no host or real-account acceptance is implied.

## 4. Controlled projection contract (P0)

### 4.1 Identity and data

One projection key is `(owner_type, owner_id, bot_id, external_memory_id)`. Scope is immutable for that key; changing scope withdraws the old memory and creates a new authoritative identity. Two different external IDs remain separate even when their text is identical. Revisions are positive monotonic integers supplied by MemoryManager for this one key.

Upsert accepts a typed envelope containing that key, `revision`, active state, `scope` (type and canonical entity ID where applicable), `kind`, bounded text, source_refs, and optional validity bounds (`valid_from`, `valid_until`). Store the envelope in existing metadata/source fields where possible. Internal UUID allocation/mapping is stable and durable; client display names, email labels, or text similarity do not determine identity. Data always remains Private and sync-disabled in this store; no privacy flag supplied by a client can enable sharing.

Delete accepts the same key, canonical scope and withdrawal revision, without the old text. An unseen-key deletion must also provide that scope from authoritative state; missing scope is rejected, not replaced by a wildcard. The managed scope is supplied by the trusted adapter, never chosen by a browser or inferred by a model. Core validates type/size, required identity, scope consistency, and supported fields; execution-relevant unknown fields are rejected. Contract tests must establish concrete input limits before implementation is considered ready; reuse existing limits where suitable rather than inventing unlimited payloads.

### 4.2 Durable idempotency and ordering

Persist one local projection-state record per key: immutable canonical scope, maximum adopted revision, live/tombstone state, and internal memory ID (allocated when first materialized). Retain scope in tombstones across deletion, restart and reactivation; compare every later mutation against it and reject changed scope even at a higher revision. An unseen delete establishes the scope from its authoritative envelope. It is a derived replay fence, not another business authority. Store it in the same SQLite transaction as the corresponding memory replacement/removal, reusing transactional storage helpers. Do not call two separately committing CRUD methods and assume atomicity.

- Higher revision replaces the live projection or writes a tombstone.
- Equal revision and equal normalized intent is a replay; return the original adopted outcome. Equal revision with different intent returns a conflict.
- Lower revision returns stale without mutating payload, index, or state. A tombstone also fences late upserts. Only a higher, explicitly reactivated authoritative revision may recreate the key.
- A duplicate delete is successful and reports already absent. A delete for a never-indexed key still persists its revision fence.
- Normalize supported semantic fields deterministically; do not compare arbitrary JSON serialization or logging/display metadata. Retain no withdrawn text merely to compare a tombstone replay.
- The adapter sends current authority, not a queued stale body: before replaying an outbox event, reload current memory/withdrawal state and authorize its scope. Delivery identities and cursors persist in PostgreSQL using the Bot's existing delivery patterns.

Core serializes a controlled mutation and its index/cache publication within one instance. A search must not observe a newly adopted revision paired with old indexed text. After SQL commit, failure to publish index/cache state makes the instance unavailable until repaired/reopened; it cannot return success and continue serving stale state. After a crash, opening reconstructs indexes from committed controlled rows before search becomes ready. No network or model work occurs under a SQLite write transaction.

Projection durability must precede acknowledgement of its replay position in PostgreSQL. Persist the replica generation and local applied replay position in the same SQLite transaction as payload/fence changes; make those changes durable with a tested controlled-store durability mode or the existing durable-flush mechanism before advancing the PostgreSQL cursor. `synchronous=NORMAL` commit by itself is not this guarantee. After restart/restore, compare local generation/applied position with the PostgreSQL cursor before readiness; if local data may have rolled back or the identities disagree, rewind to a proven retained position or rebuild. Never trust the remote cursor alone. Crash after durable local commit but before remote acknowledgement safely replays the same identities. C05/C14 must simulate local acknowledged-transaction loss with the PostgreSQL cursor surviving, not just kill a process.

### 4.3 Store and process isolation

Use a dedicated database per owner/bot and per serving-process replica. Derive paths from server-maintained identity mappings under an owner-only data directory, not caller path strings. Require owner-only file handling, including WAL/SHM, and reject symlink/path escapes; coordinate the unmerged protections noted in section 2 rather than assuming this baseline provides them. Never open a personal Cortex archive as a controlled store.

Persist a controlled-store marker. New legacy open/export/sync paths must refuse a marked controlled store, and controlled open refuses a personal store; do not rely only on caller discipline. The controlled binding exposes no general core/storage handle, legacy mutation, export, sync, or plugin access. Older binaries cannot honor a new marker, so deployment paths/access policy and rollback prohibit opening the controlled files with them.

Each replica has its own persistent replay cursor and instance ID; do not use a shared work-queue acknowledgement as proof that every replica has applied an event. P0 supports independent local replicas, not multiple processes sharing one index database. Enforce one live controlled handle per database with a process-owned POSIX file lock and an in-process registry; another handle or process fails with an explicit busy result. A process guard rejects inherited operations and new opens after a store-initialized process forks; use exec/spawn workers. Copied registry/SQLite locks are never entered in the child. This does not replace SQLite locks or imply a distributed lease.

The Bot adapter manages replay for each replica. A new/lost replica builds from current PostgreSQL state; an expired replay cursor forces rebuild. A paused or lagging replica may serve only results that survive current authoritative revalidation, otherwise the Bot uses its PostgreSQL fallback. Record bounded lag and recovery state without memory text.

Physical isolation is only the first boundary. Within the owner/bot store, each request carries a nonempty trusted set of currently allowed scopes; an empty set returns empty, never an unfiltered query. Missing scope is an error. All retrieval branches and caches, including FTS, vectors, graph expansion, keyword/semantic helpers, and cache keys, must preserve the same scope predicate. Filter before ranking/top-k and do not expose excluded IDs, snippets, counts, or metadata. If an existing branch cannot enforce the predicate, disable that branch in controlled mode until it can.

### 4.4 Search and final authority checks

`search(query, allowed_scopes, limit, budget)` returns structured hits with external identity, revision, scope, source_refs, score, and indexed text, plus explicit `degraded/truncated` information. These are internal candidates, not browser-safe responses. Enforce bounded query/result sizes and retain QueryBudget; namespace is a secondary partition, not authorization proof.

MemoryManager first resolves required defaults by exact key from current PostgreSQL state with the existing scope/precedence and validity rules, independently of Cortex readiness, hit count, or index lag. Current explicit instructions and work constraints still override defaults. Optional Cortex retrieval supplements this mandatory lookup; it cannot replace it. In particular, an execution action depending on a just-committed memory action must see that default even if no replica has indexed it yet and Cortex returns a full page of unrelated valid hits.

The adapter batch-loads current PostgreSQL memories and checks owner/bot/scope, active status, both validity bounds, source access/withdrawal, and revision before exposing anything to UI, logs, or ContextProjector. For both required exact-key defaults and optional hits, the authoritative server time must satisfy `valid_from <= now < valid_until`; a missing bound is unbounded on that side. Use timezone-aware timestamps, not browser/model clocks. Active status alone does not make a future-effective memory usable. Rejected candidates cannot enter excerpts, telemetry, summaries, or explanations. Emit current PostgreSQL text and source metadata after validation, not stale indexed text. Candidate shortage/timeout is not proof that no memory exists: fall back to the existing bounded PostgreSQL retrieval and report optional-context degradation. Required execution facts continue to come from the current work or their authoritative service.

At model-call/action boundaries, the existing Bot rules still invalidate withdrawn memory manifests, summaries, and checkpoints. Recheck both validity bounds for defaults and optional memories at these boundaries, including a memory that expires while waiting. Cortex does not claim to revoke content already sent to a model. Revalidate at the same boundary as the existing permission checks; no new distributed transaction with a model supplier is claimed.

### 4.5 Forget, rebuild, and maintenance

MemoryManager commits withdrawal and its public acknowledgement independently of index availability. All reads honor that withdrawal immediately through authoritative validation. Cortex cleanup is tracked separately: reuse core deletion, FTS triggers, index removal, cache generation and events, with the projection tombstone committed atomically. Keep retrying the same identity through the existing finite retry/operator-recovery mechanism; failed cleanup remains visible.

Rebuild constructs a separate controlled database from a consistent, authorized PostgreSQL snapshot plus a durable committed replay watermark, then applies every later change in order. Until the adapter revalidates state, the rebuilt store is not ready. Withdrawals, expirations, or permission changes during rebuild must not leak through the swap. Swap/retire handles under a local gate; pin each active query to one instance generation and drain old handles before deletion. A crash before/after the swap must reopen either a validated ready generation or a recoverable not-ready state, never a half-built database.

Do not discard tombstones while their revision range can still be replayed. A rebuild may omit old tombstones only when the old replica is retired, its generation/cursor cannot publish again, a new snapshot/replay boundary is established, and every later delivery is resolved from current authority. Backup restoration follows the same reconciliation before readiness. Local projection files, WAL/SHM, staging files and retired generations stay owner-only and are included in cleanup/retention inventory.

Logical withdrawal and physical erasure are distinct: no immediate wiping of existing backups or already-sent model requests is promised. State exactly what cleanup finished and what remains. No automatic device sync or export is enabled for these projections.

## 5. Python delivery and operational fallback (P0)

The Python `ControlledStore` binding exposes open/seed/finish_snapshot/reconcile/apply_batch/upsert/delete/search/status/retry_cleanup/close; exact dictionary schemas and limits are in the [API contract](../controlled-memory-api.md). Errors distinguish invalid scope/input, stale revision, equal-revision conflict, busy/not-ready store, and bounded retrieval degradation. Search results must not require parsing a generated prompt or tuple text to recover identity.

P0 runtime uses a supported wheel installed in a clean environment, not an adjacent Rust checkout. Add a built-wheel smoke/integration test covering import/class name, controlled APIs, restart, and scope/delete behavior. Rust workspace CI excludes cortex-python and therefore cannot substitute for this test. Verify the host's actual Python, OS, and architecture; add the binding job to the required integration checks rather than treating a skipped release workflow as acceptance. Coordinate macOS fixes with PR #14.

The current Python crate disables core's encrypted-db feature. Do not describe this wheel's SQLite data as encrypted at rest. Controlled deployment must use the host's existing approved private-data storage and retention controls; if those require application-level encryption, provide and test an encrypted wheel configuration before enablement. Never weaken storage policy just to make the wheel install.

Native work must not block the host event loop. Define binding GIL release/thread-safety and call it through the host's bounded worker mechanism, with concurrency, queue, and timeout limits. Cancellation cannot leave SQL committed with a permanently unpublished index. No detached unbounded indexing threads. Reuse core/query budgets, expose metadata-only health/lag/error metrics, and do not add telemetry/network dependencies to the core. Embedding initialization must be explicit; tests are offline with supplied synthetic vectors or lexical mode, and production model availability is checked before enabling optional semantic retrieval.

Cortex failure does not reject an otherwise valid Bot memory write or bypass permission checks. If Cortex is missing, not ready, or unusable, use PostgreSQL retrieval with a clear capability/degradation state. The integration feature flag defaults off. Disablement stops new controlled work and drains/closes handles; the original Bot and personal Cortex paths remain usable. Do not roll back by opening a controlled database with an older binary that does not understand its fences.

## 6. Later candidate extraction and tidy (P1, separate enablement)

Only after P0 acceptance, reuse `infer` and consolidation analysis to propose structured candidates with provenance and confidence. Current live `run_consolidation` is not a read-only preview and cannot be wired to the Bot preview button. Add a no-write analysis path sharing existing analysis helpers, or run analysis on an explicitly disposable isolated copy; test that previews do not mutate live rows or derived state.

MemoryManager remains responsible for accepting/rejecting a candidate, preserving explicit user corrections and causal source ordering. Consolidation cannot activate speculation, overwrite a user-confirmed value, or merge scopes. Any accepted change returns through the P0 projection path. Tidy apply/undo and expected revisions reuse the upstream Bot contract rather than introducing a second report or approval protocol.

Before enabling derived candidates, persist sufficient parent memory/input identities and revisions to invalidate all derived facts, preferences, summaries and proposals when a source is corrected, forgotten or loses access. Failure to establish that lineage keeps P1 disabled. People graph, cross-Bot sharing, and device/cloud sync remain separate proposals, not an automatic consequence of enabling P1.

## 7. Work packages and exits

Implement on isolated branches against current main, preserving others' work. Each implementation PR carries its own final diff review/CI; review of this plan does not approve later code.

| Package | Owner and code seams | Deliverable and exit |
| --- | --- | --- |
| W0: contract fixtures | Cortex + chatbot-agent owners; types and adapter interface | Freeze typed envelope, input limits, errors, allowed-scope predicate, replica/rebuild sequence and supported wheel target; document Bot adoption; agree synthetic fixtures |
| W1: controlled core | Cortex `types.rs`, `lib.rs`, storage trait/SQLite, index/retrieval | Implement projection-state transaction, controlled open/mutation/read lifecycle and fail-closed guards; C01-C10 and C12 pass |
| W2: binding/distribution | Cortex Python binding, its README, existing build/release workflows | Expose one supported surface; correct import example; installed-wheel C11/C13 pass; retain existing public API behavior |
| W3: Bot adapter | chatbot-agent memory/deliveries/ContextProjector and fixtures | Implement per-replica delivery/reconciliation, authority gate, PostgreSQL fallback and feature flag; C14-C17 pass with real test PostgreSQL |
| W4: host verification | chatbot-agent + Studio existing vendor/release flow | Install reviewed package in isolated host; C18 plus upstream memory/context regression pass; authorized rollout/rollback evidence |
| W5: candidate enhancement | Cortex + MemoryManager, separately reviewed | P1 no-write candidate preview and lineage; C19/C20 pass before any activation |

W1 and W2 follow W0; W3 can use contract fixtures while W1/W2 develop, but integration waits for both. W4 waits for W1-W3. W5 is optional after W4 and cannot block the P0 retrieval release.

## 8. Acceptance matrix and ownership

Use synthetic memory identities/text, real temporary SQLite and real test PostgreSQL where indicated. No production archive import, private model photos, paid generation, or publishing is necessary. Failure injection uses actual process termination and real files/transactions, not mocks in place of storage acceptance.

| ID | Scenario | Required result |
| --- | --- | --- |
| C01 | Controlled single/batch projection; same text under different IDs/scopes | No automatic facts/preferences/people/sync, dedup merge, decay or consolidation; independent IDs and permissions preserved |
| C02 | Replay same envelope; equal revision with different text/scope/source intent; lower revision | Replay is stable; conflicting intent rejected; lower revision has no effect |
| C03 | Commit rev 2 while rev 1 is delayed; restart between delivery and acknowledgement | Only rev 2 visible; original identities reused; persisted state and payload agree |
| C04 | Delete unseen key with/without scope; repeat delete; delayed upsert; restart then higher reactivation in same/different scope | Missing scope rejected; durable scoped tombstone; idempotent deletion and stale-write rejection; only authoritative higher reactivation in original scope succeeds, changed scope rejected |
| C05 | Kill writer before SQL commit, after commit/before index publication, and after publication/before response; simulate loss of an acknowledged local transaction | Reopen either original or fully committed state; readiness requires generation/replay-position reconciliation; no mixed revisions/text or silently skipped lost projection |
| C06 | Concurrent search/update/delete, cache hit, injected publication failure | Results bind one valid state; instance fails closed on publication failure; SQL/index/cache agree after repair |
| C07 | Same text/IDs in two owners and two Bots; path manipulation; two processes opening one replica DB | No cross-store data; safe owner-only paths; shared-handle attempts rejected and restart releases lock |
| C08 | Entity scope A/B within one Bot across every retrieval branch and warm cache | Allowed predicate holds before top-k; no excluded snippets/IDs/counts; empty scopes return empty, missing scope errors |
| C09 | Inspect structured search results; budget exhaustion and malformed oversized inputs | Identity/version/source/scope intact; bounded results; degraded search is explicit; validation is deterministic |
| C10 | Delete then inspect SQL, FTS, vector index/cache; restart | No queryable withdrawn content; fence remains without old plaintext; cleanup failures reported rather than claimed complete |
| C11 | Install built wheel in clean supported host environment, then upsert/search/delete/reopen | Correct module/class and structured types; no checkout dependency; no missing controlled APIs |
| C12 | Existing personal core/MCP/HTTP/sync/gateway regressions | Legacy calls and opt-in behavior preserved; controlled data cannot be exported or synced through legacy entrypoints |
| C13 | Native call under host concurrency, timeout/cancellation, embedding unavailable, encrypted-storage policy | Event loop stays responsive; bounded work; no publication gap; offline mode usable; required at-rest controls verified |
| C14 | Real test PG commit plus lost event/ack, restart, two replicas at different cursors, retention expiry; PG cursor ahead of restored local data | Local durable position precedes remote ack; each replica reconciles independently, rewinds/rebuilds on mismatch; stale replicas only return authority-approved results or fallback |
| C15 | Correct/forget/revoke/expire after indexing or during search; future-effective active defaults/hits before/at activation and at expiry; rejected source access | Current PG gate enforces both validity bounds for exact-key defaults and optional hits, including model-call/action boundaries; authoritative text only, no rejected content reaches logs/UI/context |
| C16 | Rebuild from empty with concurrent changes, permission revocation, stale delivery, kill/swap, restore backup | New snapshot/replay generation valid; no resurrection/leak; half-built or obsolete generation never becomes ready |
| C17 | Core unavailable, flag off, slow/short search, invalid authority lookup; just-confirmed required default missing from delayed replica that returns a full page of other valid hits | Mandatory exact-key PG defaults always precede optional search; dependent action uses the committed default; fallback is truthful, writes retained, invalid permissions fail closed |
| C18 | Host installed package, feature toggle, worker restart, rollback, new-work recall and forget | Verify actual package/config/process and end-to-end behavior; upstream M01-M06/N01-N03/L08 and existing work/approval regressions pass |
| C19 | Candidate inference and tidy preview with explicit correction/conflicting scope | No live mutation; suggestion retains provenance; no overwrite, activation or cross-scope merge |
| C20 | Forget/correct/revoke parent while preview or derived processing is delayed | Descendants invalidated; replay/undo cannot restore withdrawn evidence; P0 behavior unchanged |

An acceptance subagent must be context-isolated from implementation: give it this plan and public schemas/docs, not implementation diffs or unit tests. Implementers write unit tests separately. Adversarial review is a third independent context, per repository rules.

Cortex-side automated evidence includes the independent installed-wheel acceptance suite (31 cases exercising Cortex-side portions of C01-C13 and local crash/restore parts of C16), core unit fault injection and legacy regression suites. The public timing-based crash cases establish restart consistency; separate native test-only barriers deterministically terminate a real subprocess inside the transaction and after commit before cache invalidation/response. These establish process-crash behavior, not power-loss durability or a real-PG replay handshake. C14-C18 still require the Bot/Studio adapter and real PostgreSQL; no passing local test substitutes for those.

## 9. Review, CI, rollout and evidence

For this implementation, validate local links, fences, acceptance IDs and `git diff --check`, plus the required source and installed-wheel checks. No Bot/Studio business acceptance is claimed. Keep the exact reviewed document content and head/base/tree identifiers with raw model outputs in a traceable review record. Each model first reviews independently; cross-check only after both initial findings are saved. Fix every valid actionable finding at every severity, then repeat both reviews over the entire final diff; disputed findings require the originating reviewer to confirm closure.

Before any implementation merge, require two different underlying reviewer models independently clean on the final complete head/base and all required local CI on that same integration version. Recheck head/base, conflicts, findings and checks immediately before merging with exact-head constraint. Follow the authorized admin merge policy only after these gates; do not bypass failed/missing/skipped required checks. A main failure or base movement requires investigation and fresh evidence, not inherited clean conclusions.

Required implementation verification includes `cargo test --workspace --exclude cortex-python --exclude cortex-wasm`, the current required Clippy/privacy jobs, installed-wheel tests, and the integration matrix above for that package. Relevant existing snapshots/golden/compatibility assertions must be checked; do not rerecord baselines to mask regressions. UI remains outside this plan; any later UI change needs desktop/mobile and affected flows.

Rollout uses a clean main containing the latest release and the existing package/vendor process: test environment first, then explicitly authorized scope. Record Cortex/chatbot-agent/Studio SHAs, wheel identity, flags, storage/replica configuration, CI runs, each reviewer model/round, fault-injection results and unresolved limits. Verify actual installed package, processes, health, replay lag, cleanup failures, logs and recall/correction/forget behavior after release. Separate implementation, isolated tests, deployment and real authorized end-to-end acceptance; anything not deployed must be labeled not deployed.

Cortex-side completion covers W1-W2: controlled storage/search/lifecycle, binding and installed-wheel tests. Full Bot P0 still requires host-owned W3-W4 and real-PG C14-C18 evidence. C19-C20/P1 remain disabled. See the public API contract for caller responsibilities; this code does not grant production-operation or data-sharing authorization.
