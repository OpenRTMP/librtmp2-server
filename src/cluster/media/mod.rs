//! Inter-node media mesh.

use std::sync::Arc;

use crate::cluster::NodeId;

pub mod cache;
pub mod hub;
pub mod ownership;
pub mod peer;
pub mod protocol;
pub mod subscription;
pub mod timeline;

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

pub use hub::MediaHub;
pub use ownership::OwnershipTracker;
pub use protocol::{MEDIA_PROTOCOL_VERSION, MediaMessage};
