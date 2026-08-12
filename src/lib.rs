//! Reusable OnyxDB core and wire-protocol components.
//!
//! The public surface is experimental while the project is pre-1.0. Server
//! bootstrap, persistence, and replication lifecycle remain owned by the
//! `onyxdb` binary until their invariants have dedicated module boundaries.

pub mod client;
pub mod command;
pub mod engine;
pub mod protocol;
pub mod resp;
