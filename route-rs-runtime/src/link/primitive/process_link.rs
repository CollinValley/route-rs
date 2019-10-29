use crate::link::{Link, LinkBuilder, PacketStream, ProcessLinkBuilder};
use crate::processor::Processor;
use futures::{Async, Poll, Stream};

/// `ProcessLink` is a simple links used to process packets through the user-defined processor
/// It does not have the ability to store packets internally, and is pull-based. So it only does
/// work when it is called, and must either immediatly return recieved packets, or drop them.  This
/// link is used for transformation logic.
#[derive(Default)]
pub struct ProcessLink<P: Processor> {
    in_stream: Option<PacketStream<P::Input>>,
    processor: Option<P>,
}

impl<P: Processor> ProcessLink<P> {
    pub fn new() -> Self {
        ProcessLink {
            in_stream: None,
            processor: None,
        }
    }

    pub fn ingressor(self, in_stream: PacketStream<P::Input>) -> Self {
        ProcessLink {
            in_stream: Some(in_stream),
            processor: self.processor,
        }
    }
}

/// Although `Link` allows an arbitrary number of ingressors and egressors, `ProcessLink`
/// may only have one ingress and egress stream since it lacks some kind of queue
/// storage. In the future we might decide to restrict the interface for this link
/// for clearer intent.
impl<P: Processor + Send + 'static> LinkBuilder<P::Input, P::Output> for ProcessLink<P> {
    fn ingressors(self, mut in_streams: Vec<PacketStream<P::Input>>) -> Self {
        assert_eq!(
            in_streams.len(),
            1,
            "ProcessLink may only take 1 input stream"
        );

        ProcessLink {
            in_stream: Some(in_streams.remove(0)),
            processor: self.processor,
        }
    }

    fn build_link(self) -> Link<P::Output> {
        if self.in_stream.is_none() {
            panic!("Cannot build link! Missing input streams");
        } else if self.processor.is_none() {
            panic!("Cannot build link! Missing processor");
        } else {
            let processor = ProcessRunner::new(self.in_stream.unwrap(), self.processor.unwrap());
            (vec![], vec![Box::new(processor)])
        }
    }
}

impl<P: Processor + Send + 'static> ProcessLinkBuilder<P> for ProcessLink<P> {
    fn processor(self, processor: P) -> Self {
        ProcessLink {
            in_stream: self.in_stream,
            processor: Some(processor),
        }
    }
}

/// The single egressor of ProcessLink
struct ProcessRunner<P: Processor> {
    in_stream: PacketStream<P::Input>,
    processor: P,
}

impl<P: Processor> ProcessRunner<P> {
    fn new(in_stream: PacketStream<P::Input>, processor: P) -> Self {
        ProcessRunner {
            in_stream,
            processor,
        }
    }
}

impl<P: Processor> Stream for ProcessRunner<P> {
    type Item = P::Output;
    type Error = ();

    /// Intro to `Stream`s:
    /// 4 cases: `Async::Ready(Some)`, `Async::Ready(None)`, `Async::NotReady`, `Err`
    ///
    /// `Async::Ready(Some)`: We have a packet ready to process from the upstream processor.
    /// It's passed to our core's process function for... processing
    ///
    /// `Async::Ready(None)`: The input_stream doesn't have anymore input. Semantically,
    /// it's like an iterator has exhausted it's input. We should return `Ok(Async::Ready(None))`
    /// to signify to our downstream components that there's no more input to process.
    /// Our Processors should rarely return `Async::Ready(None)` since it will effectively
    /// kill the Stream chain.
    ///
    /// `Async::NotReady`: There is more input for us to process, but we can't make any more
    /// progress right now. The contract for Streams asks us to register with a Reactor so we
    /// will be woken up again by an Executor, but we will be relying on Tokio to do that for us.
    /// This case is handled by the `try_ready!` macro, which will automatically return
    /// `Ok(Async::NotReady)` if the input stream gives us NotReady.
    ///
    /// `Err`: is also handled by the `try_ready!` macro.
    ///
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match try_ready!(self.in_stream.poll()) {
                None => return Ok(Async::Ready(None)),
                Some(input_packet) => {
                    // if `processor.process` returns None, do nothing, loop around and try polling again.
                    if let Some(output_packet) = self.processor.process(input_packet) {
                        return Ok(Async::Ready(Some(output_packet)));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::{Drop, Identity, TransformFrom};
    use crate::utils::test::harness::run_link;
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use core::time;

    #[test]
    #[should_panic]
    fn panics_when_built_without_input_streams() {
        let identity_processor: Identity<i32> = Identity::new();

        ProcessLink::new()
            .processor(identity_processor)
            .build_link();
    }

    #[test]
    #[should_panic]
    fn panics_when_built_without_processor() {
        ProcessLink::<Identity<i32>>::new()
            .ingressor(immediate_stream(vec![]))
            .build_link();
    }

    #[test]
    fn builder_methods_work_in_any_order() {
        let packets: Vec<i32> = vec![];

        ProcessLink::new()
            .ingressor(immediate_stream(packets.clone()))
            .processor(Identity::new())
            .build_link();

        ProcessLink::new()
            .processor(Identity::new())
            .ingressor(immediate_stream(packets.clone()))
            .build_link();
    }

    #[test]
    fn identity() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let link = ProcessLink::new()
            .ingressor(immediate_stream(packets.clone()))
            .processor(Identity::new())
            .build_link();

        let results = run_link(link);
        assert_eq!(results[0], packets);
    }

    #[test]
    fn wait_between_packets() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];
        let packet_generator = PacketIntervalGenerator::new(
            time::Duration::from_millis(10),
            packets.clone().into_iter(),
        );

        let link = ProcessLink::new()
            .ingressor(Box::new(packet_generator))
            .processor(Identity::new())
            .build_link();

        let results = run_link(link);
        assert_eq!(results[0], packets);
    }

    #[test]
    fn type_transform() {
        let packets = "route-rs".chars();
        let packet_generator = immediate_stream(packets.clone());

        let link = ProcessLink::new()
            .ingressor(packet_generator)
            .processor(TransformFrom::<char, u32>::new())
            .build_link();

        let results = run_link(link);
        let expected_output: Vec<u32> = packets.map(|p| p.into()).collect();
        assert_eq!(results[0], expected_output);
    }

    #[test]
    fn drop() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let link = ProcessLink::new()
            .ingressor(immediate_stream(packets.clone()))
            .processor(Drop::new())
            .build_link();

        let results = run_link(link);
        assert_eq!(results[0], []);
    }
}
