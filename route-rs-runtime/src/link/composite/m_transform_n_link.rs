use crate::link::{
    primitive::{ForkLink, JoinLink, ProcessLink},
    Link, LinkBuilder, PacketStream, ProcessLinkBuilder,
};
use crate::processor::Processor;

#[derive(Default)]
pub struct MtransformNLink<P: Processor + Send> {
    in_streams: Option<Vec<PacketStream<P::Input>>>,
    processor: Option<P>,
    join_queue_capacity: usize,
    fork_queue_capacity: usize,
    num_egressors: Option<usize>,
}

impl<P: Processor + Send> MtransformNLink<P> {
    /// Changes join_queue_capcity, default value is 10.
    pub fn join_queue_capacity(self, queue_capacity: usize) -> Self {
        assert!(
            queue_capacity > 0,
            format!("join_queue_capacity: {} must be > 0", queue_capacity)
        );

        MtransformNLink {
            in_streams: self.in_streams,
            processor: self.processor,
            join_queue_capacity: queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    /// Changes tee_queue_capcity, default value is 10.
    pub fn fork_queue_capacity(self, queue_capacity: usize) -> Self {
        assert!(
            queue_capacity > 0,
            format!("fork_queue_capacity: {} must be > 0", queue_capacity)
        );

        MtransformNLink {
            in_streams: self.in_streams,
            processor: self.processor,
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    pub fn num_egressors(self, num_egressors: usize) -> Self {
        assert_ne!(num_egressors, 0, "num_egressors must be > 0");

        MtransformNLink {
            in_streams: self.in_streams,
            processor: self.processor,
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: Some(num_egressors),
        }
    }
}

impl<P: Processor + Send + 'static> LinkBuilder<P::Input, P::Output> for MtransformNLink<P> {
    fn new() -> Self {
        MtransformNLink {
            in_streams: None,
            processor: None,
            join_queue_capacity: 10,
            fork_queue_capacity: 10,
            num_egressors: None,
        }
    }

    fn ingressors(self, in_streams: Vec<PacketStream<P::Input>>) -> Self {
        assert!(
            !in_streams.is_empty(),
            format!("Input streams: {} should be > 0", in_streams.len())
        );

        if self.in_streams.is_some() {
            panic!("M transform N link already has input streams")
        }

        MtransformNLink {
            in_streams: Some(in_streams),
            processor: self.processor,
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    fn ingressor(self, in_stream: PacketStream<P::Input>) -> Self {
        match self.in_streams {
            None => MtransformNLink {
                in_streams: Some(vec![in_stream]),
                processor: self.processor,
                join_queue_capacity: self.join_queue_capacity,
                fork_queue_capacity: self.fork_queue_capacity,
                num_egressors: self.num_egressors,
            },
            Some(mut existing_streams) => {
                existing_streams.push(in_stream);
                MtransformNLink {
                    in_streams: Some(existing_streams),
                    processor: self.processor,
                    join_queue_capacity: self.join_queue_capacity,
                    fork_queue_capacity: self.fork_queue_capacity,
                    num_egressors: self.num_egressors,
                }
            }
        }
    }

    fn build_link(self) -> Link<P::Output> {
        if self.in_streams.is_none() {
            panic!("Cannot build link! Missing input stream");
        } else if self.num_egressors.is_none() {
            panic!("Cannot build link! Missing number of num_egressors");
        } else if self.processor.is_none() {
            panic!("Cannot build link! Missing processor");
        } else {
            let (mut join_runnables, join_egressors) = JoinLink::new()
                .ingressors(self.in_streams.unwrap())
                .queue_capacity(self.join_queue_capacity)
                .build_link();

            let (_, process_egressors) = ProcessLink::new()
                .ingressors(join_egressors)
                .processor(self.processor.unwrap())
                .build_link();

            let (mut fork_link_runnables, fork_link_egressors) = ForkLink::new()
                .ingressors(process_egressors)
                .queue_capacity(self.fork_queue_capacity)
                .num_egressors(self.num_egressors.unwrap())
                .build_link();
            fork_link_runnables.append(&mut join_runnables);

            (fork_link_runnables, fork_link_egressors)
        }
    }
}

impl<P: Processor + Send + 'static> ProcessLinkBuilder<P> for MtransformNLink<P> {
    fn processor(self, processor: P) -> Self {
        MtransformNLink {
            in_streams: self.in_streams,
            processor: Some(processor),
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: self.num_egressors,
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::link::LinkBuilder;
    use crate::processor::TransformFrom;
    use crate::utils::test::packet_generators::immediate_stream;
    use std::net::Ipv4Addr;

    use crate::utils::test::harness::{initialize_runtime, run_link};

    #[test]
    fn transform_m_streams_on_to_n_egress_streams() {
        let packets = vec![0xDEAD_BEEF, 0xBEEF_DEAD, 0x0A00_0001, 0xFFFF_FFFF];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let mut input_streams: Vec<PacketStream<u32>> = Vec::new();
            input_streams.push(immediate_stream(packets.clone()));
            input_streams.push(immediate_stream(packets.clone()));

            let link = MtransformNLink::new()
                .num_egressors(5)
                .ingressors(input_streams)
                .processor(TransformFrom::<u32, Ipv4Addr>::new())
                .build_link();

            run_link(link).await
        });
        assert_eq!(results[0].len(), packets.len() * 2);
        assert_eq!(results[1].len(), packets.len() * 2);
        assert_eq!(results[2].len(), packets.len() * 2);
        assert_eq!(results[3].len(), packets.len() * 2);
        assert_eq!(results[4].len(), packets.len() * 2);
    }
}
