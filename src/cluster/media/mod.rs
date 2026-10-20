//! Inter-node media mesh.

use std::sync::Arc;

use crate::cluster::NodeId;

pub mod cache;
pub mod hub;
pub mod live_queue;
pub mod ownership;
pub mod peer;
pub mod protocol;
pub mod subscription;
pub mod timeline;
pub mod wire;

pub type MediaMembershipFn = Arc<dyn Fn(NodeId) -> bool + Send + Sync>;

/// Async gate for inbound `Subscribe` on the media plane (owner node only).
pub type InboundSubscribeGateFn = Arc<
    dyn Fn(
            NodeId,
            String,
            String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
        + Send
        + Sync,
>;

/// Outcome of the inbound `Subscribe` gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundSubscribeVerdict {
    /// Authorized: register the subscription and fan out.
    Allow,
    /// Authorization verdict: the peer may not subscribe, retrying will not
    /// help.
    Deny,
    /// The check could not be completed (control RPC/topology failure). Not
    /// an authorization verdict: the subscriber retries and keeps its refs.
    Transient,
}

/// Async gate for inbound `Subscribe` that separates a transient failure from
/// an authorization verdict. Preferred over [`InboundSubscribeGateFn`] when
/// set.
pub type InboundSubscribeVerdictFn = Arc<
    dyn Fn(
            NodeId,
            String,
            String,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = InboundSubscribeVerdict> + Send>>
        + Send
        + Sync,
>;

pub use hub::MediaHub;
pub use ownership::OwnershipTracker;
pub use protocol::{MEDIA_PROTOCOL_MIN_VERSION, MEDIA_PROTOCOL_VERSION, MediaMessage};
