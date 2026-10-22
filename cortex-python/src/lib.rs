use cortex_core::Cortex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Convert CortexError to PyErr.
fn to_py_err(e: cortex_core::CortexError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Main Cortex Python binding.
#[pyclass(name = "Cortex")]
struct PyCortex {
    inner: Cortex,
}

#[pymethods]
impl PyCortex {
    /// Open or create a Cortex database.
    #[new]
    fn new(py: Python<'_>, db_path: &str) -> PyResult<Self> {
        let inner = py
            .allow_threads(|| Cortex::open(db_path))
            .map_err(to_py_err)?;
        Ok(Self { inner })
    }

    fn cleanup_pending(&self, py: Python<'_>) -> PyResult<bool> {
        py.allow_threads(|| self.inner.cleanup_pending())
            .map_err(to_py_err)
    }
    fn retry_cleanup(&self, py: Python<'_>) -> PyResult<bool> {
        py.allow_threads(|| self.inner.retry_cleanup())
            .map_err(to_py_err)
    }
    /// Ingest a new memory from any channel.
    #[pyo3(signature = (text, channel, user_id=None, salience=None, embedding=None))]
    fn ingest(
        &self,
        text: &str,
        channel: &str,
        user_id: Option<&str>,
        salience: Option<f32>,
        embedding: Option<Vec<f32>>,
    ) -> PyResult<String> {
        let mem = self
            .inner
            .ingest(text, channel, user_id, salience, embedding)
            .map_err(to_py_err)?;
        Ok(mem.id.to_string())
    }

    /// Multi-signal retrieval. Returns list of (memory_id, score, content_summary).
    #[pyo3(signature = (query, limit, channel=None, person_id=None, embedding=None))]
    fn retrieve(
        &self,
        query: &str,
        limit: usize,
        channel: Option<&str>,
        person_id: Option<&str>,
        embedding: Option<Vec<f32>>,
    ) -> PyResult<Vec<(String, f32, String)>> {
        let pid = person_id
            .map(uuid::Uuid::parse_str)
            .transpose()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid UUID: {}", e)))?;

        let results = self
            .inner
            .retrieve(query, limit, channel, pid, embedding)
            .map_err(to_py_err)?;

        Ok(results
            .into_iter()
            .map(|r| {
                let summary = match &r.memory.content {
                    cortex_core::types::MemContent::Text(t) => t.clone(),
                    cortex_core::types::MemContent::Fact {
                        subject,
                        predicate,
                        object,
                    } => {
                        format!("{} {} {}", subject, predicate, object)
                    }
                    cortex_core::types::MemContent::Preference { key, value, .. } => {
                        format!("{} = {}", key, value)
                    }
                    other => format!("{:?}", other),
                };
                (r.memory.id.to_string(), r.score, summary)
            })
            .collect())
    }

    /// Generate LLM-ready context from memory state.
    #[pyo3(signature = (max_tokens, channel=None, person_id=None))]
    fn get_context(
        &self,
        max_tokens: usize,
        channel: Option<&str>,
        person_id: Option<&str>,
    ) -> PyResult<String> {
        let pid = person_id
            .map(uuid::Uuid::parse_str)
            .transpose()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid UUID: {}", e)))?;

        self.inner
            .get_context(max_tokens, channel, pid)
            .map_err(to_py_err)
    }

    /// Add or resolve a person by channel identity.
    fn add_person(&self, name: &str, channel: &str, channel_user_id: &str) -> PyResult<String> {
        let person = self
            .inner
            .add_person(name, channel, channel_user_id)
            .map_err(to_py_err)?;
        Ok(person.id.to_string())
    }

    /// Run a consolidation cycle. Returns (episodes_scanned, promoted, swept, patterns).
    fn run_consolidation(&self) -> PyResult<(usize, usize, usize, usize)> {
        let report = self.inner.run_consolidation().map_err(to_py_err)?;
        Ok((
            report.episodes_scanned,
            report.promoted_to_semantic,
            report.decayed_swept,
            report.patterns_detected,
        ))
    }

    /// Get beliefs above a confidence threshold.
    fn get_beliefs(&self, threshold: f32) -> PyResult<Vec<(String, f32)>> {
        let beliefs = self.inner.get_beliefs(threshold).map_err(to_py_err)?;
        Ok(beliefs
            .into_iter()
            .map(|b| (b.key, b.probability))
            .collect())
    }

    /// Observe evidence for a belief key.
    fn observe_belief(&self, key: &str, supports: bool, strength: f32) -> PyResult<f32> {
        let belief = self
            .inner
            .observe_belief(key, supports, strength)
            .map_err(to_py_err)?;
        Ok(belief.probability)
    }

    /// Add a semantic fact.
    fn add_fact(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        confidence: f32,
        channel: &str,
    ) -> PyResult<String> {
        let mem = self
            .inner
            .add_fact(subject, predicate, object, confidence, channel, None)
            .map_err(to_py_err)?;
        Ok(mem.id.to_string())
    }

    /// Add a user preference.
    fn add_preference(&self, key: &str, value: &str, confidence: f32) -> PyResult<String> {
        let mem = self
            .inner
            .add_preference(key, value, confidence)
            .map_err(to_py_err)?;
        Ok(mem.id.to_string())
    }
}

pyo3::create_exception!(cortex_ai_memory, ControlledError, PyRuntimeError);

fn controlled_error(py: Python<'_>, e: cortex_core::controlled::ControlledError) -> PyErr {
    let error = ControlledError::new_err(e.to_string());
    let _ = error.value(py).setattr("code", e.code());
    error
}
fn invalid_controlled(py: Python<'_>) -> PyErr {
    controlled_error(
        py,
        cortex_core::controlled::ControlledError::InvalidInput("request"),
    )
}
fn bounded_value(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    depth: usize,
    budget: &mut usize,
    list_limit: usize,
) -> PyResult<serde_json::Value> {
    use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
    let invalid = || invalid_controlled(py);
    if depth > 8 {
        return Err(invalid());
    }
    let debit = |budget: &mut usize, bytes: usize| -> PyResult<()> {
        *budget = budget.checked_sub(bytes).ok_or_else(invalid)?;
        Ok(())
    };
    debit(budget, 8)?;
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if value.is_exact_instance_of::<PyBool>() {
        return Ok(serde_json::Value::Bool(
            value.extract().map_err(|_| invalid())?,
        ));
    }
    if value.is_exact_instance_of::<PyInt>() {
        let number = value.extract::<i64>().map_err(|_| invalid())?;
        debit(budget, number.to_string().len())?;
        return Ok(serde_json::Value::from(number));
    }
    if value.is_exact_instance_of::<PyFloat>() {
        let number = value.extract::<f64>().map_err(|_| invalid())?;
        let number = serde_json::Number::from_f64(number).ok_or_else(invalid)?;
        debit(budget, number.to_string().len())?;
        return Ok(serde_json::Value::Number(number));
    }
    if value.is_exact_instance_of::<PyString>() {
        let string = value.downcast::<PyString>().map_err(|_| invalid())?;
        // Check Python's character length before asking for a UTF-8 buffer.
        if string.len().map_err(|_| invalid())? > 12000 {
            return Err(invalid());
        }
        let text = string.to_str().map_err(|_| invalid())?;
        if text.len() > 48000 {
            return Err(invalid());
        }
        let bytes = text
            .bytes()
            .map(|b| {
                if b < 32 {
                    6
                } else if b == b'"' || b == b'\\' {
                    2
                } else {
                    1
                }
            })
            .sum::<usize>();
        debit(budget, bytes)?;
        return Ok(serde_json::Value::String(text.to_owned()));
    }
    if value.is_exact_instance_of::<PyList>() {
        let list = value.downcast::<PyList>().map_err(|_| invalid())?;
        if list.len() > list_limit {
            return Err(invalid());
        }
        debit(budget, list.len())?;
        let mut result = Vec::with_capacity(list.len());
        for item in list.iter() {
            result.push(bounded_value(py, &item, depth + 1, budget, 100)?);
        }
        return Ok(serde_json::Value::Array(result));
    }
    if value.is_exact_instance_of::<PyDict>() {
        let dict = value.downcast::<PyDict>().map_err(|_| invalid())?;
        if dict.len() > 16 {
            return Err(invalid());
        }
        let mut result = serde_json::Map::new();
        for (key, item) in dict.iter() {
            if !key.is_exact_instance_of::<PyString>() {
                return Err(invalid());
            }
            let key = key.downcast::<PyString>().map_err(|_| invalid())?;
            if key.len().map_err(|_| invalid())? > 64 {
                return Err(invalid());
            }
            let key = key.to_str().map_err(|_| invalid())?;
            if key.len() > 64 {
                return Err(invalid());
            }
            debit(budget, key.len() * 6 + 3)?;
            let cap = match key {
                "embedding" => 4096,
                "source_refs" => 16,
                _ => 100,
            };
            result.insert(
                key.to_owned(),
                bounded_value(py, &item, depth + 1, budget, cap)?,
            );
        }
        return Ok(serde_json::Value::Object(result));
    }
    Err(invalid())
}
fn parse_bounded<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    cap: usize,
) -> PyResult<T> {
    let value = bounded_value(py, value, 0, &mut (8 * 1024 * 1024), cap)?;
    serde_json::from_value(value).map_err(|_| invalid_controlled(py))
}
fn parse_controlled<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<T> {
    parse_bounded(py, value, 100)
}
fn controlled_result<T: serde::Serialize + Send>(
    py: Python<'_>,
    value: Result<T, cortex_core::controlled::ControlledError>,
) -> PyResult<Py<PyAny>> {
    let value = value.map_err(|e| controlled_error(py, e))?;
    let body = py
        .allow_threads(move || serde_json::to_string(&value))
        .map_err(|_| invalid_controlled(py))?;
    Ok(py.import("json")?.call_method1("loads", (body,))?.unbind())
}

#[pyclass(name = "ControlledStore")]
struct PyControlledStore {
    creator_pid: u32,
    inner: parking_lot::Mutex<Option<std::sync::Arc<cortex_core::controlled::ControlledStore>>>,
}
impl Drop for PyControlledStore {
    fn drop(&mut self) {
        let handle = self.inner.get_mut().take();
        if self.creator_pid != std::process::id() {
            // Never touch copied SQLite/mutex state during child finalization.
            std::mem::forget(handle);
            return;
        }
        if let Some(handle) = handle {
            // PyO3 invokes pyclass destruction with an attached Python thread;
            // with_gil reuses that attachment even during interpreter teardown.
            // Native destruction performs no Python work while detached.
            Python::with_gil(|py| {
                py.allow_threads(move || {
                    let _ = handle.close();
                    drop(handle);
                })
            });
        }
    }
}

impl PyControlledStore {
    fn check_process(&self, py: Python<'_>) -> PyResult<()> {
        if self.creator_pid != std::process::id() {
            return Err(controlled_error(
                py,
                cortex_core::controlled::ControlledError::Conflict(
                    "inherited handle; open after fork",
                ),
            ));
        }
        Ok(())
    }
    fn handle(
        &self,
        py: Python<'_>,
    ) -> PyResult<std::sync::Arc<cortex_core::controlled::ControlledStore>> {
        self.check_process(py)?;
        let handle = { self.inner.lock().clone() };
        handle.ok_or_else(|| controlled_error(py, cortex_core::controlled::ControlledError::Closed))
    }
}
#[pymethods]
impl PyControlledStore {
    #[new]
    fn new(
        py: Python<'_>,
        root: &Bound<'_, PyAny>,
        owner_type: &Bound<'_, PyAny>,
        owner_id: &Bound<'_, PyAny>,
        bot_id: &Bound<'_, PyAny>,
        replica_id: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let root: String = parse_controlled(py, root)?;
        let owner_type: String = parse_controlled(py, owner_type)?;
        let owner_id: String = parse_controlled(py, owner_id)?;
        let bot_id: String = parse_controlled(py, bot_id)?;
        let replica_id: String = parse_controlled(py, replica_id)?;
        let identity = cortex_core::controlled::StoreIdentity {
            owner_type,
            owner_id,
            bot_id,
            replica_id,
        };
        let store = py
            .allow_threads(|| {
                cortex_core::controlled::ControlledStore::open(
                    std::path::Path::new(&root),
                    identity,
                )
            })
            .map_err(|e| controlled_error(py, e))?;
        Ok(Self {
            creator_pid: std::process::id(),
            inner: parking_lot::Mutex::new(Some(std::sync::Arc::new(store))),
        })
    }
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        controlled_result(py, py.allow_threads(|| s.status()))
    }
    fn seed(&self, py: Python<'_>, projections: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let p = parse_controlled(py, projections)?;
        controlled_result(py, py.allow_threads(|| s.seed(p)))
    }
    fn finish_snapshot(&self, py: Python<'_>, position: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let p = parse_controlled(py, position)?;
        controlled_result(py, py.allow_threads(|| s.finish_snapshot(p)))
    }
    fn reconcile(
        &self,
        py: Python<'_>,
        generation: &Bound<'_, PyAny>,
        position: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let p = parse_controlled(py, position)?;
        let generation: String = parse_controlled(py, generation)?;
        controlled_result(py, py.allow_threads(|| s.reconcile(&generation, p)))
    }
    fn apply_batch(
        &self,
        py: Python<'_>,
        mutations: &Bound<'_, PyAny>,
        position: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let m = parse_controlled(py, mutations)?;
        let p = parse_controlled(py, position)?;
        controlled_result(py, py.allow_threads(|| s.apply_batch(m, p)))
    }
    fn upsert(
        &self,
        py: Python<'_>,
        projection: &Bound<'_, PyAny>,
        position: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let m = parse_controlled(py, projection)?;
        let p = parse_controlled(py, position)?;
        controlled_result(py, py.allow_threads(|| s.upsert(m, p)))
    }
    fn delete(
        &self,
        py: Python<'_>,
        deletion: &Bound<'_, PyAny>,
        position: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let m = parse_controlled(py, deletion)?;
        let p = parse_controlled(py, position)?;
        controlled_result(py, py.allow_threads(|| s.delete(m, p)))
    }
    #[pyo3(signature = (query, allowed_scopes, **options))]
    fn search(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyAny>,
        allowed_scopes: &Bound<'_, PyAny>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let query: String = parse_controlled(py, query)?;
        let scopes: Vec<cortex_core::controlled::Scope> = parse_bounded(py, allowed_scopes, 64)?;
        let mut limit: usize = 10;
        let mut embedding: Option<Vec<f32>> = None;
        let mut max_candidates: usize = 1000;
        let mut max_duration_ms: u64 = 250;
        if let Some(options) = options {
            for (key, value) in options.iter() {
                let key: String = parse_controlled(py, &key)?;
                match key.as_str() {
                    "limit" => limit = parse_controlled(py, &value)?,
                    "embedding" => embedding = parse_bounded(py, &value, 4096)?,
                    "max_candidates" => max_candidates = parse_controlled(py, &value)?,
                    "max_duration_ms" => max_duration_ms = parse_controlled(py, &value)?,
                    _ => return Err(invalid_controlled(py)),
                }
            }
        }
        controlled_result(
            py,
            py.allow_threads(|| {
                s.search(
                    &query,
                    &scopes,
                    cortex_core::controlled::SearchOptions {
                        limit,
                        embedding,
                        max_candidates,
                        max_duration_ms,
                    },
                )
            }),
        )
    }
    fn retry_cleanup(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        controlled_result(py, py.allow_threads(|| s.retry_cleanup()))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.check_process(py)?;
        let handle = { self.inner.lock().take() };
        let s = handle.ok_or_else(|| {
            controlled_error(py, cortex_core::controlled::ControlledError::Closed)
        })?;
        py.allow_threads(move || {
            let result = s.close();
            drop(s);
            result
        })
        .map_err(|e| controlled_error(py, e))
    }
}

#[pymodule]
fn cortex_ai_memory(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCortex>()?;
    m.add_class::<PyControlledStore>()?;
    m.add("ControlledError", m.py().get_type::<ControlledError>())?;
    Ok(())
}
