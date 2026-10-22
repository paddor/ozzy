use crate::{
    awaitable::Awaitable,
    config::Config,
    consumer::Consumer,
    execution::{Lease, Owner, Slot, borrow, opened, wait},
    producer::Producer,
    sdk_error,
    values::Checkpoint,
};
use ozzy::{BrokerLinks, SharedTopicWriterConfig, WriterRuntime};
use pyo3::prelude::*;

#[derive(Debug)]
pub(crate) struct State {
    runtime: Option<WriterRuntime>,
    config: Config,
    links: Option<BrokerLinks>,
    closed: bool,
}

impl State {
    async fn connect(&mut self) -> PyResult<()> {
        if self.closed {
            return Err(crate::ClosedError::new_err("client is closed"));
        }
        if self.links.is_none() {
            self.links = Some(
                BrokerLinks::connect(
                    self.runtime
                        .as_ref()
                        .ok_or_else(|| crate::ClosedError::new_err("client is closed"))?,
                    self.config.links.clone(),
                )
                .await
                .map_err(sdk_error)?,
            );
        }
        Ok(())
    }
    async fn close(&mut self) -> PyResult<()> {
        self.closed = true;
        if let Some(links) = &self.links {
            links.shutdown().await.map_err(sdk_error)?;
        }
        self.links = None;
        self.runtime = None;
        Ok(())
    }
}

/// Clone the shared links for an open, so other threads and tasks may open
/// handles meanwhile. Connect and close still lease the client.
fn shared(client: &Bound<'_, Client>) -> PyResult<(BrokerLinks, SharedTopicWriterConfig)> {
    let mut this = borrow(client)?;
    let state = this.0.get_mut()?;
    let links = opened(&mut state.links)?.clone();
    Ok((links, state.config.writer.clone()))
}

type Options = (String, Option<Checkpoint>, Option<Vec<u32>>);

#[pyclass(module = "ozzy._native")]
#[derive(Debug)]
pub(crate) struct Client(Slot<State>);
impl Owner for Client {
    type State = State;
    fn slot(&mut self) -> &mut Slot<State> {
        &mut self.0
    }
}

#[pymethods]
impl Client {
    #[new]
    fn new(config: &Config) -> PyResult<Self> {
        Ok(Self(Slot::new(State {
            runtime: Some(runtime()?),
            config: config.clone(),
            links: None,
            closed: false,
        })))
    }
    fn connect(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        wait(py, lease.get_mut()?.connect(), None)?
    }
    fn aconnect(slf: Py<Self>) -> Awaitable {
        Awaitable::lifecycle(async move {
            let mut lease = Lease::take(slf)?;
            lease.get_mut()?.connect().await?;
            Ok(crate::awaitable::none())
        })
    }
    #[pyo3(signature = (topic, identity=None, takeover=false))]
    fn producer(
        slf: &Bound<'_, Self>,
        topic: String,
        identity: Option<Vec<u8>>,
        takeover: bool,
    ) -> PyResult<Producer> {
        let (links, writer) = shared(slf)?;
        let open = async move { Producer::open(&links, topic, writer, identity, takeover).await };
        wait(slf.py(), open, None)?
    }
    #[pyo3(signature = (topic, identity=None, takeover=false))]
    fn aproducer(
        slf: Py<Self>,
        topic: String,
        identity: Option<Vec<u8>>,
        takeover: bool,
    ) -> Awaitable {
        Awaitable::lifecycle(async move {
            let (links, writer) = Python::attach(|py| shared(slf.bind(py)))?;
            let producer = Producer::open(&links, topic, writer, identity, takeover).await?;
            Python::attach(|py| Py::new(py, producer).map(Py::into_any))
        })
    }
    fn consumer(slf: &Bound<'_, Self>, topic: String, options: Options) -> PyResult<Consumer> {
        let (start, checkpoint, partitions) = options;
        let (links, _) = shared(slf)?;
        let open = Consumer::open(links, topic, start, checkpoint, partitions);
        wait(slf.py(), open, None)?
    }
    fn aconsumer(slf: Py<Self>, topic: String, options: Options) -> Awaitable {
        let (start, checkpoint, partitions) = options;
        Awaitable::lifecycle(async move {
            let (links, _) = Python::attach(|py| shared(slf.bind(py)))?;
            let consumer = Consumer::open(links, topic, start, checkpoint, partitions).await?;
            Python::attach(|py| Py::new(py, consumer).map(Py::into_any))
        })
    }
    fn close(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        wait(py, lease.get_mut()?.close(), None)?
    }
    fn aclose(slf: Py<Self>) -> Awaitable {
        Awaitable::lifecycle(async move {
            let mut lease = Lease::take(slf)?;
            lease.get_mut()?.close().await?;
            Ok(crate::awaitable::none())
        })
    }
}

fn runtime() -> PyResult<WriterRuntime> {
    #[cfg(test)]
    if let Some(context) = crate::tests::CONTEXT.with(|context| context.borrow().clone()) {
        return WriterRuntime::with_context(context).map_err(sdk_error);
    }
    WriterRuntime::new().map_err(sdk_error)
}
