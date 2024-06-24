//! # What are they for?
//!
//! Links are an abstraction used by the runtime to link processors and classifiers together, to manage the flow of packets through the router.
//! Processors and Classifiers, which implement all the non-flow business logic of the router, are loaded into links to create specfic behavior.
//! Links are composed together via the `Link` trait. The `TokioRunnable` portions are handed to Tokio (some Links, such as `QueueLink`, are async
//! and rely on Tokio to pull packets from the upstream.)
//! When the library user uses Graphgen, Links are declared, connected, and placed into a runtime via generated code. The user does this by laying
//! out their desired graph of Processors and Classifiers in a graph file, and Graphgen does all the work from there! Additionally, since all links
//! must implement the LinkBuilder trait, Links are transparently composable at no cost during runtime! The most basic links that implement Poll,
//! Stream and Future are Primitives. Links that are made by wrapping a collection of Links are called Composites. Link composition is a great way
//! to reduce the complexity of your router, and make its design more inspectable. Users of the library are encouraged to create their own Composite
//! Links by composing `Primitives` and other `CompositeLinks` together. This prevents the user from having to worry about the complexities of generically
//! chaining asynchronous computation together around Channels; freeing you to focus on the business logic you would like your router to implement.

use crate::processor::Processor;

/// Composites are groups of links pre-assmebled to provide higher level functionality. They are highly customizable and users of the
/// library are encourged to make their own to encourage code reuse.
pub mod composite;

/// Primitive links individually implement all the flow based logic that can be combined to create more compilcated
/// composite links. Users of the library should not have to implement their own primitive links, but should rather combine them into
/// their own custom composite links.
pub mod primitive;

/// Commmon utilities used by links, for instance the `task_park` utility used in primitive links to facilite sleeping and waking.
pub mod utils;

/// All Links communicate through streams of packets. This allows them to be composable.
pub type PacketStream<Input> = Box<dyn futures::Stream<Item = Input> + Send + Unpin>;
/// Some Links may need to be driven by Tokio. This represents a handle to something Tokio can run.
pub type TokioRunnable = Box<dyn futures::Future<Output = ()> + Send + Unpin>;
/// LinkBuilders build this.
pub type Link<Output> = (Vec<TokioRunnable>, Vec<PacketStream<Output>>);

/// `LinkBuilder` applies a builder pattern to create `Links`! `Links` should be created this way
/// so they can be composed together
///
/// The two type parameters, Input and Output, refer to the input and output types of the Link.
/// You can tell because the ingress/egress streams are of type `PacketStream<Input>`/`PacketStream<Output>` respectively.
pub trait LinkBuilder<Input, Output> {
    /// This is a builder struct, so we want to initialize without any parameters, and then pass
    /// each of our arguments in dedicated builder methods.
    fn new() -> Self;

    /// Links need a way to receive input from upstream.
    /// Some Links such as `ProcessLink` will only need at most 1, but others can accept many.
    /// Links that can not support the number of ingressors provided will panic. Links that already have
    /// ingressors will panic on calling this function, since we expect this is a user configuration error.
    fn ingressors(self, in_streams: Vec<PacketStream<Input>>) -> Self;

    /// Append ingressor to list of ingressors, works like push() for a Vector
    /// If the link can not support the addition of another ingressor, it will panic.
    fn ingressor(self, in_stream: PacketStream<Input>) -> Self;

    /// Provides any tokio-driven Futures needed to drive the Link, as well as handles for downstream
    /// `Link`s to use. This method consumes the `Link` since we want to move ownership of a `Link`'s
    /// runnables and egressors to the caller.
    fn build_link(self) -> Link<Output>;
}

/// `ProcessLink` and `QueueLink` impl `ProcessLinkBuilder`, since they are required to have their
/// Inputs and Outputs match that of their `Processor`.
pub trait ProcessLinkBuilder<P: Processor>: LinkBuilder<P::Input, P::Output> {
    fn processor(self, processor: P) -> Self;
}

pub trait IngressLinkBuilder<Packet>: LinkBuilder<(), Packet> {
    type Receiver;

    fn channel(self, receiver: Self::Receiver) -> Self;
}

pub trait EgressLinkBuilder<Packet>: LinkBuilder<Packet, ()> {
    type Sender;

    fn channel(self, sender: Self::Sender) -> Self;
}
