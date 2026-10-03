//! Shared broker frontend components. Routing owns no partition authority.
//!
//! A live link supplies the independently established peer/session binding.
//! OMQ inproc lanes carry bounded shard commands and native data. Partition
//! actors validate complete commands; transport pressure bounds admission.

mod routing;
pub use routing::{Binding, Kind, Placement, Routed, RoutingError, RoutingTable};

mod dispatch;
pub use dispatch::{DataPressure, Dispatcher, DispatcherLimits, Rejected, Rejection, SetupError};

mod flow;
mod replies;
pub use replies::{ReplyError, ReplyLimits, ReplyProgress};

pub use crate::peer_sessions::{Handled as Negotiated, LinkIds, Sessions as LinkSessions};

mod service;
pub use service::{Access, Link, Links, ReceiveError, Service, ServiceError};

mod port;
pub use port::{
    Pending, Port, PortError, PortSetupError, ProgressError, ReplyResult, RouteError, RouteResult,
};

mod publication;
pub use publication::{PublicationError, PublicationResult};

mod buffers;
pub use buffers::{BufferError, ReceiveBuffers, ReceiveStorage};

mod data_lane;
mod inproc;
pub use data_lane::{
    DataInput, DataLaneError, DataReceiver, DataSendError, DataSender, data_channel,
};

mod watch;
pub use watch::{RouteState, WatchError, WatchLimits, WatchNotice, WatchRegistry, WatchUpdates};

mod catalog;
pub use catalog::{CatalogError, TopicCatalog};

#[cfg(test)]
mod test_support;
