use crate::link::{
    primitive::{ForkLink, JoinLink},
    Link, LinkBuilder, PacketStream,
};

#[derive(Default)]
pub struct MtoNLink<Packet: Sized + Send + Clone> {
    in_streams: Option<Vec<PacketStream<Packet>>>,
    join_queue_capacity: usize,
    fork_queue_capacity: usize,
    num_egressors: Option<usize>,
}

impl<Packet: Sized + Send + Clone> MtoNLink<Packet> {
    /// Changes join_queue_capcity, default value is 10.
    pub fn join_queue_capacity(self, queue_capacity: usize) -> Self {
        assert!(
            queue_capacity > 0,
            format!("join_queue_capacity: {}, must be > 0", queue_capacity)
        );

        MtoNLink {
            in_streams: self.in_streams,
            join_queue_capacity: queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    /// Changes tee_queue_capcity, default value is 10.
    pub fn tee_queue_capacity(self, queue_capacity: usize) -> Self {
        assert!(
            queue_capacity > 0,
            format!("tee_queue_capacity: {}, must be > 0", queue_capacity)
        );

        MtoNLink {
            in_streams: self.in_streams,
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    pub fn num_egressors(self, num_egressors: usize) -> Self {
        assert!(
            num_egressors > 0,
            format!("num_egressors: {}, must be > 0", num_egressors)
        );

        MtoNLink {
            in_streams: self.in_streams,
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: Some(num_egressors),
        }
    }
}

impl<Packet: Sized + Send + Clone + 'static> LinkBuilder<Packet, Packet> for MtoNLink<Packet> {
    fn new() -> Self {
        MtoNLink {
            in_streams: None,
            join_queue_capacity: 10,
            fork_queue_capacity: 10,
            num_egressors: None,
        }
    }

    fn ingressors(self, in_streams: Vec<PacketStream<Packet>>) -> Self {
        assert!(
            !in_streams.is_empty(),
            format!("Input streams: {} must be > 0", in_streams.len())
        );

        if self.in_streams.is_some() {
            panic!("MtoNLink already has input streams")
        }

        MtoNLink {
            in_streams: Some(in_streams),
            join_queue_capacity: self.join_queue_capacity,
            fork_queue_capacity: self.fork_queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    fn ingressor(self, in_stream: PacketStream<Packet>) -> Self {
        match self.in_streams {
            None => MtoNLink {
                in_streams: Some(vec![in_stream]),
                join_queue_capacity: self.join_queue_capacity,
                fork_queue_capacity: self.fork_queue_capacity,
                num_egressors: self.num_egressors,
            },
            Some(mut existing_streams) => {
                existing_streams.push(in_stream);
                MtoNLink {
                    in_streams: Some(existing_streams),
                    join_queue_capacity: self.join_queue_capacity,
                    fork_queue_capacity: self.fork_queue_capacity,
                    num_egressors: self.num_egressors,
                }
            }
        }
    }

    fn build_link(self) -> Link<Packet> {
        if self.in_streams.is_none() {
            panic!("Cannot build link! Missing input stream");
        } else if self.num_egressors.is_none() {
            panic!("Cannot build link! Missing number of num_egressors");
        } else {
            let (mut join_runnables, join_egressors) = JoinLink::new()
                .ingressors(self.in_streams.unwrap())
                .queue_capacity(self.join_queue_capacity)
                .build_link();
            let (mut fork_link_runnables, fork_link_egressors) = ForkLink::new()
                .ingressors(join_egressors)
                .queue_capacity(self.fork_queue_capacity)
                .num_egressors(self.num_egressors.unwrap())
                .build_link();
            fork_link_runnables.append(&mut join_runnables);
            (fork_link_runnables, fork_link_egressors)
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::link::LinkBuilder;
    use crate::utils::test::packet_generators::immediate_stream;

    use crate::utils::test::harness::{initialize_runtime, run_link};

    #[test]
    fn clone_m_streams_on_to_n_egress_streams() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 11];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let mut input_streams: Vec<PacketStream<usize>> = Vec::new();
            input_streams.push(immediate_stream(packets.clone()));
            input_streams.push(immediate_stream(packets.clone()));

            let link = MtoNLink::new()
                .num_egressors(5)
                .ingressors(input_streams)
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
