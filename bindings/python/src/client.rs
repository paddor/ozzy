use crate::{
    config::Config,
    consumer::Consumer,
    execution::{Lease, Owner, Slot, drive, opened, wait},
    producer::Producer,
    sdk_error,
    values::Checkpoint,
};
use ozzy::{BrokerLinks, WriterRuntime};
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
    async fn producer(
        &mut self,
        topic: String,
        identity: Option<Vec<u8>>,
        takeover: bool,
    ) -> PyResult<Producer> {
        Producer::open(
            opened(&mut self.links)?,
            topic,
            self.config.writer.clone(),
            identity,
            takeover,
        )
        .await
    }
    async fn consumer(
        &mut self,
        topic: String,
        start: String,
        checkpoint: Option<Checkpoint>,
        partitions: Option<Vec<u32>>,
    ) -> PyResult<Consumer> {
        Consumer::open(
            opened(&mut self.links)?.clone(),
            topic,
            start,
            checkpoint,
            partitions,
        )
        .await
    }
}

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
    fn connect(&mut self, py: Python<'_>) -> PyResult<()> {
        wait(py, self.0.get_mut()?.connect())?
    }
    async fn aconnect(slf: Py<Self>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        drive(lease.get_mut()?.connect()).await?
    }
    #[pyo3(signature = (topic, identity=None, takeover=false))]
    fn producer(
        &mut self,
        py: Python<'_>,
        topic: String,
        identity: Option<Vec<u8>>,
        takeover: bool,
    ) -> PyResult<Producer> {
        wait(py, self.0.get_mut()?.producer(topic, identity, takeover))?
    }
    #[pyo3(signature = (topic, identity=None, takeover=false))]
    async fn aproducer(
        slf: Py<Self>,
        topic: String,
        identity: Option<Vec<u8>>,
        takeover: bool,
    ) -> PyResult<Producer> {
        let mut lease = Lease::take(slf)?;
        drive(lease.get_mut()?.producer(topic, identity, takeover)).await?
    }
    fn consumer(
        &mut self,
        py: Python<'_>,
        topic: String,
        options: (String, Option<Checkpoint>, Option<Vec<u32>>),
    ) -> PyResult<Consumer> {
        let (start, checkpoint, partitions) = options;
        wait(
            py,
            self.0
                .get_mut()?
                .consumer(topic, start, checkpoint, partitions),
        )?
    }
    async fn aconsumer(
        slf: Py<Self>,
        topic: String,
        options: (String, Option<Checkpoint>, Option<Vec<u32>>),
    ) -> PyResult<Consumer> {
        let (start, checkpoint, partitions) = options;
        let mut lease = Lease::take(slf)?;
        drive(
            lease
                .get_mut()?
                .consumer(topic, start, checkpoint, partitions),
        )
        .await?
    }
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        wait(py, self.0.get_mut()?.close())?
    }
    async fn aclose(slf: Py<Self>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        drive(lease.get_mut()?.close()).await?
    }
}

fn runtime() -> PyResult<WriterRuntime> {
    #[cfg(test)]
    if let Some(context) = crate::tests::CONTEXT.with(|context| context.borrow().clone()) {
        return WriterRuntime::with_context(context).map_err(sdk_error);
    }
    WriterRuntime::new().map_err(sdk_error)
}
