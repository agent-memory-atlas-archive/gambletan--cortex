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
