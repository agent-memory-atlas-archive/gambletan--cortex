"""Independent black-box acceptance for the documented installed-wheel API.

Run outside the checkout with an installed wheel, for example:
  python -I /absolute/path/to/tests/test_controlled_store.py
No implementation imports, network, models, production data, or SQL mutations.
"""
import concurrent.futures
import copy
import datetime
import gc
import importlib.metadata
import json
import math
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import cortex_ai_memory as memory
from cortex_ai_memory import ControlledError, ControlledStore


SCOPE = {"kind": "owner", "id": "merchant-acceptance"}
OTHER = {"kind": "entity", "id": "entity-other"}


def projection(key="memory-one", revision=1, scope=None, text="orchid report preference", **extra):
    value = {"memory_id": key, "revision": revision, "scope": dict(scope or SCOPE),
             "kind": "preference", "text": text,
             "source_refs": [{"kind": "input", "provider": "acceptance", "id": "source-one"}]}
    value.update(extra)
    return value


def upsert(value):
    return {"op": "upsert", "projection": value}


def deletion(key="memory-one", revision=2, scope=None):
    return {"memory_id": key, "revision": revision, "scope": dict(scope or SCOPE)}


CHILD = r'''
import json, sys, time
from cortex_ai_memory import ControlledStore, ControlledError
root, mode = sys.argv[1:]
try:
    s = ControlledStore(root, 'merchant', 'merchant-acceptance', 'bot-one', 'replica-one')
except ControlledError as e:
    print(json.dumps({'error': e.code}), flush=True)
    sys.exit(0)
if mode == 'lock':
    print(json.dumps({'opened': True}), flush=True)
    time.sleep(60)
else:
    state = s.status()
    s.reconcile(state['generation'], state['position'])
    values = [{'op':'upsert', 'projection': {'memory_id':'crash-%d'%i, 'revision':2,
        'scope':{'kind':'owner','id':'merchant-acceptance'}, 'kind':'fact',
        'text':'orchid committed revision two ' + 'x'*11000,
        'source_refs':[{'kind':'input','provider':'acceptance','id':'crash-source'}]}}
        for i in range(100)]
    print(json.dumps({'starting': True}), flush=True)
    receipt = s.apply_batch(values, 101)
    print(json.dumps({'committed': receipt}), flush=True)
    time.sleep(60)
'''


class ControlledStoreAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="controlled-acceptance-")
        self.root = str(Path(self.temp.name).resolve() / "private")
        self.handles = []
        self.children = []

    def tearDown(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)
        for store in reversed(self.handles):
            try:
                store.close()
            except ControlledError as error:
                self.assertEqual(error.code, "closed")
        self.temp.cleanup()

    def open(self, replica="replica-one", bot="bot-one", owner="merchant-acceptance", root=None):
        value = ControlledStore(root or self.root, "merchant", owner, bot, replica)
        self.handles.append(value)
        return value

    def ready(self, values=(), position=100, **kwargs):
        store = self.open(**kwargs)
        store.seed(list(values))
        store.finish_snapshot(position)
        state = store.status()
        store.reconcile(state["generation"], position)
        return store

    def error(self, code, function, *args, **kwargs):
        with self.assertRaises(ControlledError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def hits(self, store, scopes=None, **kwargs):
        result = store.search("orchid", [SCOPE] if scopes is None else scopes, **kwargs)
        self.assertIsInstance(result["degraded"], bool)
        self.assertIsInstance(result["truncated"], bool)
        return result["hits"]

    def reconcile(self, store):
        state = store.status()
        store.reconcile(state["generation"], state["position"])
        return state

    def child(self, mode):
        process = subprocess.Popen([sys.executable, "-I", "-c", CHILD, self.root, mode],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.children.append(process)
        return process

    def line(self, process):
        lines = queue.Queue()
        reader = threading.Thread(target=lambda: lines.put(process.stdout.readline()), daemon=True)
        reader.start()
        value = lines.get(timeout=20)
        self.assertTrue(value, "child exited before its public-API checkpoint")
        return json.loads(value)

    def test_installed_distribution_and_api(self):
        module_file = Path(memory.__file__).resolve()
        checkout = Path(__file__).resolve().parents[2]
        self.assertNotIn(checkout, module_file.parents, "test requires the installed built wheel")
        distributions = importlib.metadata.packages_distributions().get("cortex_ai_memory", [])
        self.assertTrue(distributions, "native module must have installed wheel distribution metadata")
        self.assertTrue(any(importlib.metadata.distribution(name).read_text("WHEEL") for name in distributions))
        self.assertTrue(issubclass(ControlledError, Exception))
        self.assertTrue(callable(memory.Cortex))

    def test_snapshot_reconcile_restart_and_closed_lifecycle(self):
        store = self.open()
        initial = store.status()
        self.assertFalse(initial["ready"])
        self.assertFalse(initial["sealed"])
        self.error("not_ready", store.search, "orchid", [SCOPE])
        self.error("not_ready", store.reconcile, initial["generation"], initial["position"])
        store.seed([projection()])
        store.seed([projection()])
        store.finish_snapshot(100)
        store.finish_snapshot(100)
        self.error("conflict", store.finish_snapshot, 101)
        self.error("conflict", store.seed, [projection("late")])
        self.error("not_ready", store.search, "orchid", [SCOPE])
        state = self.reconcile(store)
        self.assertTrue(store.status()["ready"])
        self.assertEqual(len(self.hits(store)), 1)
        store.apply_batch([], 101)
        store.finish_snapshot(100)
        self.assertEqual(store.status()["position"], 101)
        self.assertTrue(store.status()["sealed"])
        self.assertTrue(store.status()["ready"])
        self.error("conflict", store.finish_snapshot, 101)
        self.assertEqual(store.status()["position"], 101)
        self.error("conflict", store.reconcile, state["generation"] + "-wrong", 101)
        self.assertFalse(store.status()["ready"])
        self.reconcile(store)
        self.error("conflict", store.reconcile, state["generation"], 100)
        self.assertFalse(store.status()["ready"])
        store.close()
        for call in (store.status, lambda: store.search("orchid", [SCOPE]),
                     lambda: store.apply_batch([], 101), lambda: store.seed([])):
            self.error("closed", call)
        reopened = self.open()
        self.assertFalse(reopened.status()["ready"])
        self.assertEqual(reopened.status()["generation"], state["generation"])
        self.assertEqual(reopened.status()["position"], 101)
        self.reconcile(reopened)
        self.assertEqual(self.hits(reopened)[0]["memory_id"], "memory-one")

    def test_revisions_normalized_replay_and_sequential_positions(self):
        store = self.ready()
        item = projection(revision=2, source_refs=[
            {"kind": "input", "provider": "acceptance", "id": "b"},
            {"kind": "input", "provider": "acceptance", "id": "a"}])
        receipt = store.upsert(item, 101)
        self.assertTrue(receipt["durable"])
        self.assertEqual(receipt["position"], 101)
        self.assertNotIn(item["text"], json.dumps(receipt))
        normalized = copy.deepcopy(item)
        normalized["source_refs"].reverse()
        self.assertEqual(store.apply_batch([upsert(normalized)], 101), receipt)
        changed = dict(item, text="orchid conflicting text")
        self.error("conflict", store.upsert, changed, 101)
        self.error("conflict", store.upsert, changed, 102)
        self.assertEqual(store.status()["position"], 101)
        stale = store.upsert(projection(revision=1), 102)
        self.assertEqual(stale["outcomes"][0]["outcome"], "stale")
        replay = store.upsert(normalized, 103)
        self.assertEqual(replay["outcomes"][0]["outcome"], "replayed")
        self.error("conflict", store.apply_batch, [], 105)
        self.error("conflict", store.apply_batch, [], 102)
        empty = store.apply_batch([], 104)
        self.assertEqual(empty["outcomes"], [])
        self.assertEqual(store.apply_batch([], 104), empty)
        self.error("conflict", store.upsert, projection("different"), 104)
        hit = self.hits(store)[0]
        self.assertEqual((hit["revision"], hit["text"]), (2, item["text"]))

    def test_tombstone_scope_and_unseen_delete_survive_restart(self):
        store = self.ready([projection()])
        store.delete(deletion(), 101)
        self.assertEqual(self.hits(store), [])
        self.assertEqual(store.upsert(projection(), 102)["outcomes"][0]["outcome"], "stale")
        self.error("conflict", store.upsert, projection(revision=3, scope=OTHER), 103)
        self.error("invalid_input", store.delete, {"memory_id": "unseen", "revision": 5}, 103)
        store.delete(deletion("unseen", 5, OTHER), 103)
        store.close()
        store = self.open()
        self.reconcile(store)
        self.error("conflict", store.upsert, projection("unseen", 6), 104)
        self.assertEqual(store.upsert(projection("unseen", 4, OTHER), 104)["outcomes"][0]["outcome"], "stale")
        duplicate = store.delete(deletion("unseen", 5, OTHER), 105)
        self.assertEqual(duplicate["outcomes"][0]["outcome"], "replayed")
        store.upsert(projection("unseen", 6, OTHER), 106)
        store.upsert(projection(revision=3), 107)
        self.assertEqual({h["memory_id"] for h in self.hits(store, [SCOPE, OTHER])}, {"memory-one", "unseen"})

    def test_batch_atomicity_invalid_second_and_conflict_second(self):
        store = self.ready([projection()])
        invalid = projection("invalid", revision=True)
        self.error("invalid_input", store.apply_batch,
                   [upsert(projection("first")), upsert(invalid)], 101)
        self.assertEqual(store.status()["position"], 100)
        self.assertEqual({h["memory_id"] for h in self.hits(store)}, {"memory-one"})
        self.error("conflict", store.apply_batch,
                   [upsert(projection("first")), upsert(projection(text="orchid changed"))], 101)
        self.assertEqual(store.status()["position"], 100)
        store.upsert(projection("first"), 101)
        self.assertEqual(len(self.hits(store)), 2)

    def test_scope_filter_precedes_topk_and_cache_and_vectors(self):
        excluded = [projection("excluded-%02d" % i, scope=OTHER,
                               text="orchid " * 20, embedding=[1.0, 0.0]) for i in range(20)]
        included = [projection("allowed-a", embedding=[0.9, 0.1]),
                    projection("allowed-b", scope={"kind": "project", "id": "project-a"}, embedding=[0.8, 0.2])]
        store = self.ready(excluded + included)
        allowed = [SCOPE, included[1]["scope"]]
        for vector in (None, [1.0, 0.0], None):
            hits = self.hits(store, allowed, limit=2, embedding=vector)
            self.assertEqual({h["memory_id"] for h in hits}, {"allowed-a", "allowed-b"})
            for hit in hits:
                self.assertEqual((hit["owner_type"], hit["owner_id"], hit["bot_id"]),
                                 ("merchant", "merchant-acceptance", "bot-one"))
                self.assertIn(hit["scope"], allowed)
                self.assertEqual(hit["revision"], 1)
                self.assertEqual(hit["source_refs"], included[0]["source_refs"])
                self.assertIsInstance(hit["score"], (int, float))
        self.assertEqual(self.hits(store, []), [])
        with self.assertRaises((TypeError, ControlledError)):
            store.search("orchid")
        bounded = store.search("orchid", [SCOPE, OTHER], max_candidates=1)
        self.assertTrue(bounded["degraded"] or bounded["truncated"])

    def test_validity_roundtrip_and_interval_validation(self):
        item = projection(valid_from="2000-01-01T00:00:00Z", valid_until="2090-01-01T00:00:00Z")
        store = self.ready([item])
        # Native hits are candidates; current-time authorization belongs to PG.
        hit = self.hits(store)[0]
        self.assertEqual(hit["valid_from"], item["valid_from"])
        self.assertEqual(hit["valid_until"], item["valid_until"])
        for start, end in (("2031-01-01T00:00:00Z", "2030-01-01T00:00:00Z"),
                           ("2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z"),
                           ("not-a-timestamp", "2031-01-01T00:00:00Z")):
            with self.subTest(start=start, end=end):
                self.error("invalid_input", store.upsert,
                           projection("bad-interval", valid_from=start, valid_until=end), 101)
        store.upsert(projection("unbounded"), 101)
        hit = next(h for h in self.hits(store) if h["memory_id"] == "unbounded")
        self.assertIsNone(hit["valid_from"])
        self.assertIsNone(hit["valid_until"])

    def test_input_bounds_unknown_fields_and_integer_precision(self):
        store = self.ready()
        bad_items = [projection(revision=r) for r in (True, 1.5, 0, -1, 2**63)]
        bad_items += [projection(memory_id=""), projection(memory_id="x"*257),
                      projection(memory_id="line\nbreak"), projection(text="x"*12001),
                      projection(source_refs=[]), projection(extra_unknown=True),
                      projection(scope={"kind": "owner", "id": "another-owner"}),
                      projection(embedding=[math.nan]), projection(embedding=[math.inf]),
                      projection(embedding=[]), projection(embedding=[0.0]*4097)]
        for item in bad_items:
            with self.subTest(item_fields=list(item), revision=item["revision"]):
                error = self.error("invalid_input", store.upsert, item, 101)
                self.assertNotIn("orchid report preference", str(error))
                self.assertEqual(store.status()["position"], 100)
        for position in (True, 1.5, -1, 2**63):
            self.error("invalid_input", store.apply_batch, [], position)
        for kwargs in ({"limit": 0}, {"limit": 101}, {"max_candidates": 0},
                       {"max_candidates": 10001}, {"max_duration_ms": 0}, {"max_duration_ms": 251}):
            self.error("invalid_input", store.search, "orchid", [SCOPE], **kwargs)
        self.error("invalid_input", store.search, "", [SCOPE])
        self.error("invalid_input", store.search, "x"*4001, [SCOPE])
        self.error("invalid_input", store.search, "orchid", [OTHER]*65)
        self.error("invalid_input", store.apply_batch, [upsert(projection(str(i))) for i in range(101)], 101)
        large = 2**53 + 19
        store.upsert(projection(revision=large, embedding=[1.0, 0.0]), 101)
        self.assertEqual(self.hits(store)[0]["revision"], large)
        self.error("invalid_input", store.upsert, projection("wrong-dimension", embedding=[1.0]), 102)
        other = self.ready(position=large, replica="large-position")
        other.apply_batch([], large+1)
        self.assertEqual(other.status()["position"], large+1)

    def test_owner_bot_replica_isolation_and_private_files(self):
        first = self.ready([projection()])
        bot = self.ready([projection("other-bot", text="orchid bot independent")], bot="bot-two")
        owner_scope = {"kind": "owner", "id": "merchant-two"}
        owner = self.ready([projection("other-owner", scope=owner_scope)], owner="merchant-two")
        replica = self.ready(replica="replica-two")
        self.assertEqual([h["memory_id"] for h in self.hits(first)], ["memory-one"])
        self.assertEqual([h["memory_id"] for h in self.hits(bot)], ["other-bot"])
        self.assertEqual([h["memory_id"] for h in self.hits(owner, [owner_scope])], ["other-owner"])
        self.assertEqual(self.hits(replica), [])
        self.assertNotEqual(first.status()["generation"], replica.status()["generation"])
        for path in Path(self.root).rglob("*"):
            self.assertFalse(path.stat().st_mode & 0o077, "store artifact must be owner-only: " + str(path))
        link = Path(self.temp.name) / "symlink-root"
        link.symlink_to(self.root, target_is_directory=True)
        self.error("invalid_input", self.open, root=str(link))
        self.error("invalid_input", self.open, root=self.root + "/../escape")

    def test_personal_cortex_refuses_controlled_database(self):
        store = self.ready([projection()])
        store.close()
        # Inspect only fixture file headers to find the generated SQLite path;
        # generated path layout and internal tables are deliberately unknown.
        databases = []
        for path in Path(self.root).rglob("*"):
            if path.is_file():
                with path.open("rb") as handle:
                    if handle.read(16) == b"SQLite format 3\x00":
                        databases.append(path)
        self.assertEqual(len(databases), 1)
        with self.assertRaises(RuntimeError):
            personal = memory.Cortex(str(databases[0]))
            del personal
            gc.collect()
        reopened = self.open()
        self.reconcile(reopened)
        self.assertEqual(len(self.hits(reopened)), 1)

    def test_native_validity_defense_in_depth(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp = lambda delta: (now + datetime.timedelta(days=delta)).isoformat()
        store = self.ready([
            projection("current", valid_from=stamp(-1), valid_until=stamp(1)),
            projection("future", valid_from=stamp(1), valid_until=stamp(2)),
            projection("expired", valid_from=stamp(-2), valid_until=stamp(-1)),
        ])
        self.assertEqual({h["memory_id"] for h in self.hits(store)}, {"current"})

    def test_large_semantic_search_obeys_budget_and_preserves_responsiveness(self):
        store = self.open()
        vector = [0.01 * ((i % 7) + 1) for i in range(128)]
        for offset in range(0, 1100, 100):
            store.seed([projection("budget-%d" % i, embedding=vector)
                        for i in range(offset, offset + 100)])
        store.finish_snapshot(100)
        self.reconcile(store)
        begin = time.monotonic()
        result = store.search("orchid", [SCOPE], limit=100, embedding=vector,
                              max_candidates=1100, max_duration_ms=250)
        elapsed = time.monotonic() - begin
        # Allow substantial CI scheduling/serialization overhead beyond the
        # native budget; this catches runaway work, not a real-time SLA.
        self.assertLess(elapsed, 2.0, "semantic search exceeded bounded-work allowance")
        self.assertLessEqual(len(result["hits"]), 100)
        self.assertIsInstance(result["degraded"], bool)
        self.assertIsInstance(result["truncated"], bool)
        if elapsed >= 0.250:
            self.assertTrue(result["degraded"] or result["truncated"],
                            "budget exhaustion must be explicit")
        self.assertTrue(all(hit["scope"] == SCOPE for hit in result["hits"]))
        begin = time.monotonic()
        receipt = store.upsert(projection("after-budget-search", embedding=vector), 101)
        self.assertLess(time.monotonic() - begin, 2.0,
                        "search left later mutation unresponsive")
        self.assertTrue(receipt["durable"])
        self.assertEqual(store.status()["position"], 101)
        begin = time.monotonic()
        store.close()
        self.assertLess(time.monotonic() - begin, 2.0,
                        "search left close unresponsive")

    def test_long_native_operation_releases_gil(self):
        store = self.open()
        values = [projection("gil-%d" % i, text="orchid " + "x"*11990,
                             embedding=[0.001]*4096) for i in range(100)]
        stopped = threading.Event()
        started = threading.Event()
        ticks = []
        def heartbeat():
            started.set()
            while not stopped.is_set():
                ticks.append(time.monotonic())
                time.sleep(0.0005)
        worker = threading.Thread(target=heartbeat)
        worker.start()
        started.wait(timeout=5)
        old_interval = sys.getswitchinterval()
        try:
            # Prevent normal Python bytecode switching from masquerading as a
            # GIL release inside this one deliberately substantial native call.
            sys.setswitchinterval(10.0)
            begin = time.monotonic()
            store.seed(values)
            end = time.monotonic()
        finally:
            sys.setswitchinterval(old_interval)
            stopped.set()
            worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertTrue(any(begin < tick < end for tick in ticks),
                        "heartbeat made no progress during the native seed call")
        store.finish_snapshot(100)
        self.reconcile(store)
        self.assertEqual(len(self.hits(store, limit=100)), 100)

    def test_process_and_inprocess_lock_released_on_termination(self):
        store = self.ready()
        self.error("busy", self.open)
        competing = self.child("lock")
        self.assertEqual(self.line(competing), {"error": "busy"})
        competing.wait(timeout=10)
        store.close()
        holder = self.child("lock")
        self.assertEqual(self.line(holder), {"opened": True})
        self.error("busy", self.open)
        holder.kill()
        holder.wait(timeout=10)
        reopened = self.open()
        self.assertFalse(reopened.status()["ready"])
        self.reconcile(reopened)

    def test_threaded_queries_mutations_and_close_drain(self):
        store = self.ready([projection()])
        start = threading.Event()
        results = []
        def searcher():
            start.wait()
            for _ in range(50):
                try:
                    for hit in self.hits(store):
                        self.assertIn((hit["revision"], hit["text"]),
                                      {(1, "orchid report preference"), (2, "orchid revision two")})
                except ControlledError as error:
                    self.assertEqual(error.code, "closed")
                    return
            results.append(True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(searcher) for _ in range(4)]
            start.set()
            store.upsert(projection(revision=2, text="orchid revision two"), 101)
            store.close()
            for future in futures:
                future.result(timeout=20)
        reopened = self.open()
        self.reconcile(reopened)
        self.assertEqual(self.hits(reopened)[0]["revision"], 2)

    def test_child_crash_after_commit_and_during_write(self):
        for phase in ("committed", "inflight"):
            with self.subTest(phase=phase):
                root = str((Path(self.temp.name) / phase).resolve())
                store = self.ready([projection("crash-%d" % i) for i in range(100)], root=root)
                original_generation = store.status()["generation"]
                store.close()
                original_root = self.root
                self.root = root
                try:
                    writer = self.child("write")
                    self.assertEqual(self.line(writer), {"starting": True})
                    if phase == "committed":
                        receipt = self.line(writer)["committed"]
                        self.assertTrue(receipt["durable"])
                    writer.kill()
                    writer.wait(timeout=10)
                    reopened = self.open()
                    state = reopened.status()
                    self.assertFalse(state["ready"])
                    self.assertEqual(state["generation"], original_generation)
                    self.assertIn(state["position"], (100, 101))
                    if phase == "committed":
                        self.assertEqual(state["position"], 101)
                    self.error("not_ready", reopened.search, "orchid", [SCOPE])
                    self.reconcile(reopened)
                    hits = self.hits(reopened, limit=100)
                    self.assertEqual(len(hits), 100)
                    expected_revision = 2 if state["position"] == 101 else 1
                    self.assertEqual({h["revision"] for h in hits}, {expected_revision})
                    for hit in hits:
                        self.assertEqual(hit["text"].startswith("orchid committed revision two"), expected_revision == 2)
                    reopened.close()
                finally:
                    self.root = original_root

    def test_cleanup_pending_with_readonly_reader_then_retry_and_reopen(self):
        store = self.ready([projection()])
        self.assertIsInstance(store.status()["cleanup_pending"], bool)
        databases = []
        for path in Path(self.root).rglob("*.db"):
            with path.open("rb") as handle:
                if handle.read(16) == b"SQLite format 3\x00":
                    databases.append(path)
        self.assertEqual(len(databases), 1)
        blocker = r'''
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1] + '?mode=ro', uri=True)
connection.execute('BEGIN')
connection.execute('SELECT count(*) FROM sqlite_master').fetchone()
print('ready', flush=True)
sys.stdin.readline()
connection.close()
'''
        reader = subprocess.Popen([sys.executable, "-I", "-c", blocker, databases[0].as_uri()],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)
        self.children.append(reader)
        ready_lines = queue.Queue()
        threading.Thread(target=lambda: ready_lines.put(reader.stdout.readline()), daemon=True).start()
        self.assertEqual(ready_lines.get(timeout=10).strip(), "ready")
        try:
            receipt = store.upsert(projection(revision=2, text="orchid replacement"), 101)
            self.assertTrue(receipt["durable"])
            self.assertTrue(store.status()["cleanup_pending"])
            begin = time.monotonic()
            self.assertTrue(store.retry_cleanup()["cleanup_pending"])
            self.assertLess(time.monotonic() - begin, 5.0)
            withdrawn = store.delete(deletion(revision=3), 102)
            self.assertTrue(withdrawn["durable"])
            self.assertTrue(store.status()["cleanup_pending"])
            self.assertEqual(self.hits(store), [])
        finally:
            reader.communicate(input="release\n", timeout=10)
            self.assertEqual(reader.returncode, 0)
        begin = time.monotonic()
        self.assertFalse(store.retry_cleanup()["cleanup_pending"])
        self.assertLess(time.monotonic() - begin, 5.0)
        self.assertEqual(store.delete(deletion(revision=3), 102), withdrawn)
        store.close()
        reopened = self.open()
        self.assertFalse(reopened.status()["cleanup_pending"])
        self.reconcile(reopened)
        self.assertEqual(self.hits(reopened), [])

    def test_search_malformed_options_builtin_only_and_unicode(self):
        store = self.ready([projection(text="İabout 猫 orchid")])
        result = store.search("İabout 猫", [SCOPE])
        self.assertIsInstance(result["hits"], list)
        with self.assertRaises(TypeError):
            store.search("orchid", [SCOPE], 10)
        for name in ("limit", "max_candidates", "max_duration_ms"):
            for value in (True, False, "10", None, 1.5, 2**100, -1, [], {}):
                with self.subTest(option=name, value_type=type(value).__name__):
                    self.error("invalid_input", store.search, "orchid", [SCOPE], **{name: value})
        for vector in (True, "vector", [True], [1e308], [1.0, object()]):
            self.error("invalid_input", store.search, "orchid", [SCOPE], embedding=vector)
        self.error("invalid_input", store.search, " ".join("term%d" % i for i in range(65)), [SCOPE])
        class Custom:
            pass
        class DictSubclass(dict):
            pass
        class ListSubclass(list):
            pass
        class StringSubclass(str):
            pass
        class IntSubclass(int):
            pass
        for value in (Custom(), DictSubclass(projection()),
                      projection(text=StringSubclass("orchid")), projection(revision=IntSubclass(2))):
            self.error("invalid_input", store.upsert, value, 101)
        self.error("invalid_input", store.seed, ListSubclass([projection("new")]))
        cyclic = []
        cyclic.append(cyclic)
        self.error("invalid_input", store.seed, cyclic)
        deep = []
        for _ in range(12):
            deep = [deep]
        self.error("invalid_input", store.seed, deep)
        self.error("invalid_input", store.upsert, projection(text="x"*(8*1024*1024+1)), 101)
        duplicate = projection("duplicate")
        self.error("invalid_input", store.apply_batch, [upsert(duplicate), upsert(duplicate)], 101)
        self.assertEqual(store.status()["position"], 100)

    def test_exact_submillisecond_interval_and_clock_filter(self):
        start = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=0.15)
        until = start + datetime.timedelta(microseconds=200)
        item = projection("precise", valid_from=start.isoformat(), valid_until=until.isoformat())
        store = self.ready([item])
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            before = datetime.datetime.now(datetime.timezone.utc)
            hits = self.hits(store)
            after = datetime.datetime.now(datetime.timezone.utc)
            for hit in hits:
                self.assertEqual(datetime.datetime.fromisoformat(hit["valid_from"].replace("Z", "+00:00")), start)
                self.assertEqual(datetime.datetime.fromisoformat(hit["valid_until"].replace("Z", "+00:00")), until)
                self.assertLess(before, until, "expired submillisecond candidate returned")
                self.assertGreaterEqual(after, start, "future submillisecond candidate returned")
            if before > until + datetime.timedelta(milliseconds=5):
                self.assertEqual(hits, [])
                break
        else:
            self.fail("precise interval test failed to reach its expiry checkpoint")
        # Distinct bounds inside one millisecond must remain a valid interval.
        tiny = projection("nanosecond", valid_from="2000-01-01T00:00:00.000000001Z",
                          valid_until="2000-01-01T00:00:00.000000002Z")
        store.upsert(tiny, 101)
        self.assertEqual(self.hits(store), [])

    def test_unsafe_nonsticky_ancestor_rejected(self):
        ancestor = Path(self.temp.name).resolve() / "unsafe-parent"
        ancestor.mkdir(mode=0o700)
        try:
            for mode in (0o770, 0o707, 0o777):
                ancestor.chmod(mode)
                self.error("invalid_input", self.open, root=str(ancestor / "private"))
        finally:
            ancestor.chmod(0o700)

    def test_fork_inherited_handle_fails_before_copied_locks(self):
        script = r'''
import json, os, signal, sys
from cortex_ai_memory import ControlledStore, ControlledError
s = ControlledStore(sys.argv[1], 'merchant', 'merchant-acceptance', 'bot-one', 'replica-one')
s.finish_snapshot(100)
s.reconcile(s.status()['generation'], 100)
r, w = os.pipe()
pid = os.fork()
if pid == 0:
    signal.alarm(5)
    os.close(r)
    codes = []
    try:
        for call in (s.status, lambda:s.search('orchid', []), s.retry_cleanup, s.close):
            try:
                call()
                codes.append('unexpected-success')
            except ControlledError as e:
                codes.append(e.code)
        os.write(w, json.dumps(codes).encode())
    finally:
        os._exit(0)
os.close(w)
data = os.read(r, 65536)
os.waitpid(pid, 0)
print(data.decode(), flush=True)
s.close()
'''
        completed = subprocess.run([sys.executable, "-I", "-c", script, self.root],
                                   capture_output=True, text=True, timeout=15)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout), ["conflict"]*4)
        reopened = self.open()
        self.reconcile(reopened)

    def test_immediate_reopen_while_old_thread_calls_are_queued(self):
        store = self.ready([projection()])
        barrier = threading.Barrier(9)
        def caller():
            barrier.wait(timeout=10)
            for _ in range(100):
                try:
                    store.search("orchid", [SCOPE])
                except ControlledError as error:
                    self.assertEqual(error.code, "closed")
                    return
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(caller) for _ in range(8)]
            barrier.wait(timeout=10)
            store.close()
            # Reopen before waiting for old Python callers to finish: none may
            # reacquire ownership or touch the newly opened generation.
            reopened = self.open()
            self.reconcile(reopened)
            reopened.upsert(projection(revision=2, text="orchid reopened"), 101)
            for future in futures:
                future.result(timeout=15)
            self.assertEqual(self.hits(reopened)[0]["text"], "orchid reopened")

    def test_unauthorized_term_load_cannot_change_authorized_ranking(self):
        allowed = [projection("allowed-exact", text="orchid orchid report", embedding=[1.0, 0.0]),
                   projection("allowed-other", text="orchid other report", embedding=[0.0, 1.0])]
        store = self.open()
        store.seed(allowed)
        store.finish_snapshot(100)
        self.reconcile(store)
        baseline = store.search("orchid report", [SCOPE], limit=2, max_candidates=2, embedding=[1.0, 0.0])
        signature = [(hit["memory_id"], hit["score"]) for hit in baseline["hits"]]
        self.assertEqual({key for key, _ in signature}, {"allowed-exact", "allowed-other"})
        position = 100
        for offset in range(0, 1500, 100):
            position += 1
            store.apply_batch([upsert(projection("excluded-load-%d" % i, scope=OTHER,
                                                 text="orchid "*30 + "report"))
                               for i in range(offset, offset+100)], position)
        for _ in range(3):
            result = store.search("orchid report", [SCOPE], limit=2, max_candidates=2, embedding=[1.0, 0.0])
            self.assertEqual([(hit["memory_id"], hit["score"]) for hit in result["hits"]], signature)
            self.assertEqual(result["degraded"], baseline["degraded"])
            self.assertEqual(result["truncated"], baseline["truncated"])

    def test_personal_fifo_is_rejected_without_blocking(self):
        fifo = Path(self.temp.name).resolve() / "not-a-database.db"
        os.mkfifo(fifo, 0o600)
        script = r'''
import sys
from cortex_ai_memory import Cortex
try:
    instance = Cortex(sys.argv[1])
except RuntimeError:
    print('refused', flush=True)
else:
    raise AssertionError('personal Cortex accepted FIFO')
'''
        begin = time.monotonic()
        completed = subprocess.run([sys.executable, "-I", "-c", script, str(fifo)],
                                   capture_output=True, text=True, timeout=4.0)
        self.assertLess(time.monotonic() - begin, 5.0)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "refused")
        self.assertTrue(fifo.is_fifo())

    def test_personal_owned_file_under_unsafe_ancestor_is_refused(self):
        ancestor = Path(self.temp.name).resolve() / "personal-unsafe-parent"
        ancestor.mkdir(mode=0o700)
        database = ancestor / "personal.db"
        database.write_bytes(b"")
        database.chmod(0o600)
        self.assertEqual(database.stat().st_uid, os.geteuid())
        before = (database.stat().st_mode, database.read_bytes())
        ancestor.chmod(0o777)
        try:
            with self.assertRaises(RuntimeError):
                instance = memory.Cortex(str(database))
                del instance
                gc.collect()
            self.assertEqual((database.stat().st_mode, database.read_bytes()), before)
            self.assertEqual({path.name for path in ancestor.iterdir()}, {"personal.db"})
        finally:
            ancestor.chmod(0o700)

    def test_aggregate_numeric_input_budget_is_atomic(self):
        store = self.ready()
        vector = [1.234567890123456e-100] * 4096
        oversized = [upsert(projection("numeric-budget-%d" % i, embedding=vector))
                     for i in range(100)]
        self.assertGreater(len(json.dumps(oversized).encode("utf-8")), 8*1024*1024,
                           "fixture must exceed aggregate JSON budget")
        self.error("invalid_input", store.apply_batch, oversized, 101)
        self.assertEqual(store.status()["position"], 100)
        self.assertEqual(self.hits(store), [])
        receipt = store.upsert(projection("legitimate-vector", embedding=vector), 101)
        self.assertTrue(receipt["durable"])
        self.assertEqual(self.hits(store)[0]["memory_id"], "legitimate-vector")

    def test_sticky_parent_precreated_final_symlink_cannot_escape(self):
        base = Path(self.temp.name).resolve()
        sticky = base / "shared-sticky"
        outside = base / "outside-root"
        sticky.mkdir(mode=0o700)
        sticky.chmod(0o1777)
        outside.mkdir(mode=0o700)
        sentinel = outside / "sentinel"
        sentinel.write_bytes(b"unchanged external fixture")
        root = sticky / "controlled-root"
        root.symlink_to(outside, target_is_directory=True)
        before = (outside.stat().st_mode, sentinel.stat().st_mode, sentinel.read_bytes())
        try:
            self.error("invalid_input", self.open, root=str(root))
            self.assertTrue(root.is_symlink())
            self.assertEqual({path.name for path in outside.iterdir()}, {"sentinel"})
            self.assertEqual((outside.stat().st_mode, sentinel.stat().st_mode, sentinel.read_bytes()), before)
        finally:
            sticky.chmod(0o700)

    def test_fork_child_liveness_does_not_retain_parent_replica_lock(self):
        script = r'''
import json, os, signal, subprocess, sys
from cortex_ai_memory import ControlledStore, ControlledError
root = sys.argv[1]
s = ControlledStore(root, 'merchant', 'merchant-acceptance', 'bot-one', 'replica-one')
s.finish_snapshot(100)
r, w = os.pipe()
release_r, release_w = os.pipe()
pid = os.fork()
if pid == 0:
    signal.alarm(10)
    os.close(r); os.close(release_w)
    codes = []
    for call in (s.status, lambda: ControlledStore(root, 'merchant', 'merchant-acceptance', 'bot-one', 'fork-new')):
        try:
            call(); codes.append('unexpected-success')
        except ControlledError as error:
            codes.append(error.code)
    os.write(w, (json.dumps(codes)+'\n').encode())
    os.close(w)
    os.read(release_r, 1)
    os._exit(0)
os.close(w); os.close(release_r)
try:
    codes = json.loads(os.fdopen(r).readline())
    assert codes == ['conflict', 'conflict'], codes
    s.close()
    reopened = ControlledStore(root, 'merchant', 'merchant-acceptance', 'bot-one', 'replica-one')
    assert os.waitpid(pid, os.WNOHANG) == (0, 0), 'child must remain alive during reopen'
    reopened.close()
    fresh = "from cortex_ai_memory import ControlledStore; import sys; s=ControlledStore(sys.argv[1], 'merchant', 'merchant-acceptance', 'bot-one', 'spawn-new'); s.close(); print('fresh-opened')"
    spawned = subprocess.run([sys.executable, '-I', '-c', fresh, root], capture_output=True, text=True, timeout=5)
    assert spawned.returncode == 0, spawned.stderr
    assert spawned.stdout.strip() == 'fresh-opened'
    print(json.dumps({'inherited': codes, 'parent_reopened': True, 'fresh_spawn': True}), flush=True)
finally:
    os.write(release_w, b'x'); os.close(release_w)
    os.waitpid(pid, 0)
'''
        completed = subprocess.run([sys.executable, "-I", "-c", script, self.root],
                                   capture_output=True, text=True, timeout=15)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout),
                         {"inherited": ["conflict", "conflict"], "parent_reopened": True, "fresh_spawn": True})

    def test_last_reference_teardown_heartbeat_and_normal_shutdown(self):
        script = r'''
import gc, json, pathlib, subprocess, sys, threading, time
from cortex_ai_memory import ControlledStore
root, mode = sys.argv[1:]
s = ControlledStore(root, 'merchant', 'merchant-acceptance', 'bot-one', 'replica-one')
sc = {'kind':'owner','id':'merchant-acceptance'}
items = [{'memory_id':'drop-%d'%i,'revision':1,'scope':sc,'kind':'fact',
          'text':'orchid '+('x'*11990),'source_refs':[{'kind':'input','provider':'test','id':'drop'}]} for i in range(100)]
s.seed(items); s.finish_snapshot(100); s.reconcile(s.status()['generation'],100)
if mode == 'shutdown':
    print('live-store-at-shutdown', flush=True)
else:
    db = next(p for p in pathlib.Path(root).rglob('*.db') if p.read_bytes()[:16] == b'SQLite format 3\x00')
    reader_code = "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]+'?mode=ro',uri=True); c.execute('BEGIN'); c.execute('SELECT count(*) FROM sqlite_master').fetchone(); print('ready',flush=True); sys.stdin.readline(); c.close()"
    reader = subprocess.Popen([sys.executable,'-I','-c',reader_code,db.as_uri()],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    try:
        assert reader.stdout.readline().strip() == 'ready'
        for item in items:
            item['revision'] = 2
            item['text'] = 'orchid replacement '+('y'*11980)
        s.apply_batch([{'op':'upsert','projection':p} for p in items],101)
        assert s.status()['cleanup_pending']
        stop = threading.Event(); started = threading.Event(); ticks = []
        def beat():
            started.set()
            while not stop.is_set():
                ticks.append(time.monotonic()); time.sleep(0.0002)
        thread = threading.Thread(target=beat); thread.start(); started.wait()
        previous = sys.getswitchinterval()
        try:
            sys.setswitchinterval(10)
            begin = time.monotonic(); del s; gc.collect(); end = time.monotonic()
        finally:
            sys.setswitchinterval(previous); stop.set(); thread.join(timeout=2)
        assert end-begin < 5, ('unbounded destructor',end-begin)
        progress = sum(begin < tick < end for tick in ticks)
        if end-begin >= 0.005:
            assert progress > 0, ('GIL blocked through substantial teardown',end-begin)
        reopened = ControlledStore(root,'merchant','merchant-acceptance','bot-one','replica-one')
        reopened.close()
        print(json.dumps({'elapsed':end-begin,'heartbeat_ticks':progress,'reopened':True}),flush=True)
    finally:
        reader.communicate(input='release\n',timeout=5)
'''
        for mode in ("drop", "shutdown"):
            with self.subTest(mode=mode):
                root = str((Path(self.temp.name) / mode).resolve())
                completed = subprocess.run([sys.executable, "-I", "-c", script, root, mode],
                                           capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                if mode == "drop":
                    self.assertTrue(json.loads(completed.stdout)["reopened"])
                    print("last-reference teardown metrics: " + completed.stdout.strip(), flush=True)
                else:
                    self.assertEqual(completed.stdout.strip(), "live-store-at-shutdown")
                reopened = self.open(root=root)
                self.reconcile(reopened)
                self.assertEqual(len(self.hits(reopened, limit=100)), 100)
                reopened.close()

    def test_lexical_matching_uses_decoded_text_not_serialization(self):
        scores = []
        for index, text in enumerate(("alpha orchid", "alpha\norchid", "alpha\torchid", "alpha\\orchid")):
            store = self.ready([projection(text=text, embedding=[1.0, 0.0])], replica="decoded-%d" % index)
            def score(query):
                hits = store.search(query, [SCOPE], limit=1, max_candidates=1, embedding=[1.0, 0.0])["hits"]
                self.assertEqual(len(hits), 1)
                return hits[0]["score"]
            actual = score("orchid")
            unrelated = score("nonexistentlexicalword")
            self.assertGreater(actual, unrelated)
            self.assertEqual(score("Text"), unrelated,
                             "serialized Text label must not boost unrelated text")
            self.assertEqual(score("norchid"), unrelated,
                             "newline escape must not create phantom word")
            self.assertEqual(score("torchid"), unrelated,
                             "tab escape must not create phantom word")
            scores.append(actual)
        for value in scores[1:]:
            self.assertAlmostEqual(value, scores[0], places=6)
        actual_label = self.ready([projection(text="Text orchid", embedding=[1.0, 0.0])], replica="actual-label")
        match = actual_label.search("Text", [SCOPE], limit=1, max_candidates=1, embedding=[1.0, 0.0])["hits"][0]
        unrelated = actual_label.search("nonexistentlexicalword", [SCOPE], limit=1, max_candidates=1,
                                      embedding=[1.0, 0.0])["hits"][0]
        self.assertGreater(match["score"], unrelated["score"])

    def test_structured_objects_reject_positional_lists_without_mutation(self):
        scope_list = ["owner", SCOPE["id"]]
        source_list = ["input", "bot", "id"]
        projected = projection(revision=2, text="orchid positional replacement")
        projection_list = [projected["memory_id"], projected["revision"], projected["scope"],
                           projected["kind"], projected["text"], projected["source_refs"],
                           None, None, None]
        deletion_list = ["memory-one", 2, dict(SCOPE)]
        cases = [
            ("allowed_scope", lambda store: store.search("orchid", [scope_list])),
            ("projection_scope", lambda store: store.upsert(dict(projected, scope=scope_list), 101)),
            ("source_ref", lambda store: store.upsert(dict(projected, source_refs=[source_list]), 101)),
            ("deletion_scope", lambda store: store.delete(dict(deletion(), scope=scope_list), 101)),
            ("projection", lambda store: store.upsert(projection_list, 101)),
            ("deletion", lambda store: store.delete(deletion_list, 101)),
            ("batch_projection", lambda store: store.apply_batch([upsert(projection_list)], 101)),
            ("batch_deletion", lambda store: store.apply_batch(
                [{"op": "delete", "deletion": deletion_list}], 101)),
        ]
        for index, (name, call) in enumerate(cases):
            store = self.ready([projection()], replica="dict-shape-%d" % index)
            with self.subTest(case=name, check="invalid_input"):
                self.error("invalid_input", call, store)
            # Separate subtests retain state-integrity evidence even if the
            # malformed call unexpectedly succeeds instead of raising.
            with self.subTest(case=name, check="position_unchanged"):
                self.assertEqual(store.status()["position"], 100)
            with self.subTest(case=name, check="payload_unchanged"):
                hits = self.hits(store)
                self.assertEqual([(hit["memory_id"], hit["revision"], hit["text"]) for hit in hits],
                                 [("memory-one", 1, "orchid report preference")])

    def test_restored_local_transaction_loss_rejects_remote_cursor(self):
        store = self.ready([projection()])
        state = store.status()
        store.close()
        backup = Path(self.temp.name) / "backup"
        shutil.copytree(self.root, backup)
        store = self.open()
        self.reconcile(store)
        store.upsert(projection(revision=2, text="orchid durable later text"), 101)
        remote_acknowledged_position = store.status()["position"]
        store.close()
        shutil.rmtree(self.root)
        shutil.copytree(backup, self.root)
        restored = self.open()
        self.assertEqual(restored.status()["position"], 100)
        self.assertFalse(restored.status()["ready"])
        self.error("conflict", restored.reconcile, state["generation"], remote_acknowledged_position)
        self.error("not_ready", restored.search, "orchid", [SCOPE])
        # A proven retained replay may repair the replica before reconciliation.
        restored.upsert(projection(revision=2, text="orchid durable later text"), 101)
        restored.reconcile(state["generation"], remote_acknowledged_position)
        self.assertEqual(self.hits(restored)[0]["revision"], 2)


if __name__ == "__main__":
    unittest.main()
