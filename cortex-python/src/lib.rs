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
    fn new(db_path: &str) -> PyResult<Self> {
        let inner = Cortex::open(db_path).map_err(to_py_err)?;
        Ok(Self { inner })
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
            .map(|s| uuid::Uuid::parse_str(s))
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
            .map(|s| uuid::Uuid::parse_str(s))
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
fn parse_controlled<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<T> {
    let body: String = (|| -> PyResult<_> {
        py.import("json")?
            .call_method1("dumps", (value,))?
            .extract()
    })()
    .map_err(|_| invalid_controlled(py))?;
    if body.len() > 8 * 1024 * 1024 {
        return Err(invalid_controlled(py));
    }
    serde_json::from_str(&body).map_err(|_| invalid_controlled(py))
}
fn controlled_result<T: serde::Serialize>(
    py: Python<'_>,
    value: Result<T, cortex_core::controlled::ControlledError>,
) -> PyResult<Py<PyAny>> {
    let value = value.map_err(|e| controlled_error(py, e))?;
    let body = serde_json::to_string(&value).map_err(|_| invalid_controlled(py))?;
    Ok(py.import("json")?.call_method1("loads", (body,))?.unbind())
}

#[pyclass(name = "ControlledStore")]
struct PyControlledStore {
    inner: parking_lot::Mutex<Option<std::sync::Arc<cortex_core::controlled::ControlledStore>>>,
}
impl PyControlledStore {
    fn handle(
        &self,
        py: Python<'_>,
    ) -> PyResult<std::sync::Arc<cortex_core::controlled::ControlledStore>> {
        self.inner
            .lock()
            .clone()
            .ok_or_else(|| controlled_error(py, cortex_core::controlled::ControlledError::Closed))
    }
}
#[pymethods]
impl PyControlledStore {
    #[new]
    fn new(
        py: Python<'_>,
        root: String,
        owner_type: String,
        owner_id: String,
        bot_id: String,
        replica_id: String,
    ) -> PyResult<Self> {
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
        generation: String,
        position: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let p = parse_controlled(py, position)?;
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
    #[pyo3(signature = (query, allowed_scopes, limit=10, embedding=None, max_candidates=1000, max_duration_ms=250))]
    #[allow(clippy::too_many_arguments)]
    fn search(
        &self,
        py: Python<'_>,
        query: String,
        allowed_scopes: &Bound<'_, PyAny>,
        limit: i64,
        embedding: Option<Vec<f32>>,
        max_candidates: i64,
        max_duration_ms: i64,
    ) -> PyResult<Py<PyAny>> {
        let s = self.handle(py)?;
        let scopes: Vec<cortex_core::controlled::Scope> = parse_controlled(py, allowed_scopes)?;
        if limit < 0 || max_candidates < 0 || max_duration_ms < 0 {
            return Err(invalid_controlled(py));
        }
        controlled_result(
            py,
            py.allow_threads(|| {
                s.search(
                    &query,
                    &scopes,
                    cortex_core::controlled::SearchOptions {
                        limit: limit as usize,
                        embedding,
                        max_candidates: max_candidates as usize,
                        max_duration_ms: max_duration_ms as u64,
                    },
                )
            }),
        )
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let s = self.inner.lock().take().ok_or_else(|| {
            controlled_error(py, cortex_core::controlled::ControlledError::Closed)
        })?;
        py.allow_threads(move || {
            s.close();
            drop(s);
        });
        Ok(())
    }
}

#[pymodule]
fn cortex_ai_memory(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCortex>()?;
    m.add_class::<PyControlledStore>()?;
    m.add("ControlledError", m.py().get_type::<ControlledError>())?;
    Ok(())
}
