//! Python bindings to the existing Rust SDK. No Python transport or log state.
#![forbid(unsafe_code)]

mod awaitable;
mod client;
mod config;
mod consumer;
mod execution;
mod producer;
mod values;

use pyo3::{prelude::*, types::IntoPyDict};

pyo3::create_exception!(
    ozzy,
    OzzyError,
    pyo3::exceptions::PyException,
    "Rust SDK failure. An unknown write outcome must be resolved using record identity."
);
pyo3::create_exception!(
    ozzy,
    ClosedError,
    OzzyError,
    "Operation on a closed handle."
);
pyo3::create_exception!(
    ozzy,
    BusyError,
    OzzyError,
    "Another asynchronous operation exclusively leases this handle."
);

fn sdk_error(error: impl std::fmt::Display) -> PyErr {
    OzzyError::new_err(error.to_string())
}

#[pymodule(name = "_native")]
fn native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<config::Config>()?;
    module.add_class::<client::Client>()?;
    module.add_class::<producer::Producer>()?;
    module.add_class::<producer::PendingRecord>()?;
    module.add_class::<consumer::Consumer>()?;
    module.add_class::<values::Receipt>()?;
    module.add_class::<values::Record>()?;
    module.add_class::<values::Checkpoint>()?;
    module.add("OzzyError", module.py().get_type::<OzzyError>())?;
    module.add("ClosedError", module.py().get_type::<ClosedError>())?;
    module.add("BusyError", module.py().get_type::<BusyError>())?;
    // Children inherit SDK owners whose threads did not survive the fork.
    let forked = pyo3::wrap_pyfunction!(execution::forked, module)?;
    module.py().import("os")?.call_method(
        "register_at_fork",
        (),
        Some(&[("after_in_child", forked)].into_py_dict(module.py())?),
    )?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests;
