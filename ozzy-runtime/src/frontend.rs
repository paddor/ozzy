//! Shared broker frontend components. Routing owns no partition authority.
//!
//! A live link supplies the independently established peer/session binding.
//! Ordinary OMQ receive feeds metadata routing; bounded destination grants and
//! fanrings govern admission. Partition actors still validate complete commands.

mod routing;
pub use routing::{Binding, Kind, Placement, Routed, RoutingError, RoutingTable};

mod dispatch;
pub use dispatch::{
    Dispatcher, DispatcherLimits, GrantSpec, GrantTarget, Rejected, Rejection, SetupError, Subject,
};

mod flow;
mod replies;
pub use replies::{ReplyError, ReplyLimits, ReplyProgress};

pub use crate::peer_sessions::{Handled as Negotiated, LinkIds, Sessions as LinkSessions};

mod service;
pub use service::{Access, Link, Links, ReceiveError, Service, ServiceError};

mod demand;
pub use demand::{GrantRequest, GrantRequests};

mod intake;
pub use intake::{Destination, IntakeError, IntakeMessage, ReceiveSize, ShardIntake};

mod port;
pub use port::{
    InstallResult, Pending, Port, PortError, ProgressError, ReplyResult, RouteError, RouteResult,
};

mod publication;
pub use publication::{PublicationError, PublicationResult};

mod buffers;
pub use buffers::{BufferError, ReceiveBuffers, ReceiveStorage};

mod watch;
pub use watch::{RouteState, WatchError, WatchLimits, WatchNotice, WatchRegistry, WatchUpdates};

mod catalog;
pub use catalog::{CatalogError, TopicCatalog};

#[cfg(test)]
mod test_support;
