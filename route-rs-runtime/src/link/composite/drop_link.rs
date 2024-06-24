use crate::link::primitive::ProcessLink;
use crate::link::{Link, LinkBuilder, PacketStream, ProcessLinkBuilder};
use crate::processor::Drop;

/// Link that drops packets.
/// Can specify a weighted uniform distribution for dropping,
/// otherwise, will drop all incoming packets.
#[derive(Default)]
pub struct DropLink<I> {
    in_stream: Option<PacketStream<I>>,
    drop_chance: Option<f64>,
    seed: Option<u64>,
}

impl<I> DropLink<I> {
    pub fn drop_chance(self, chance: f64) -> Self {
        DropLink {
            in_stream: self.in_stream,
            drop_chance: Some(chance),
            seed: self.seed,
        }
    }

    pub fn seed(self, int_seed: u64) -> Self {
        DropLink {
            in_stream: self.in_stream,
            drop_chance: self.drop_chance,
            seed: Some(int_seed),
        }
    }
}

impl<I: Send + Clone + 'static> LinkBuilder<I, I> for DropLink<I> {
    fn new() -> Self {
        DropLink {
            in_stream: None,
            drop_chance: None,
            seed: None,
        }
    }

    fn ingressors(self, mut ingress_streams: Vec<PacketStream<I>>) -> Self {
        assert_eq!(
            ingress_streams.len(),
            1,
            "DropLink can only take 1 ingress stream"
        );

        if self.in_stream.is_some() {
            panic!("DropLink can only take 1 input stream")
        }

        DropLink {
            in_stream: Some(ingress_streams.remove(0)),
            drop_chance: self.drop_chance,
            seed: self.seed,
        }
    }

    fn ingressor(self, in_stream: PacketStream<I>) -> Self {
        if self.in_stream.is_some() {
            panic!("DropLink can only take 1 input stream")
        }

        DropLink {
            in_stream: Some(in_stream),
            drop_chance: self.drop_chance,
            seed: self.seed,
        }
    }

    fn build_link(self) -> Link<I> {
        if self.in_stream.is_none() {
            panic!("Cannot build link! Missing input streams");
        } else {
            let mut dropper: Drop<I> = Drop::new();

            if let Some(dc) = self.drop_chance {
                dropper = dropper.drop_chance(dc);
            }

            if let Some(s) = self.seed {
                dropper = dropper.seed(s);
            }

            ProcessLink::new()
                .ingressor(self.in_stream.unwrap())
                .processor(dropper)
                .build_link()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classifier::even_link;
    use crate::utils::test::harness::{initialize_runtime, run_link};
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use core::time;

    #[test]
    #[should_panic]
    fn panics_if_no_input_stream_provided() {
        DropLink::<()>::new().build_link();
    }

    #[test]
    fn finishes() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = DropLink::new()
                .ingressor(immediate_stream(packets))
                .build_link();

            run_link(link).await
        });
        assert_eq!(results[0], vec![]);
    }

    #[test]
    fn finishes_with_wait() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator =
                PacketIntervalGenerator::new(time::Duration::from_millis(10), packets.into_iter());

            let link = DropLink::new()
                .ingressor(Box::new(packet_generator))
                .build_link();

            run_link(link).await
        });
        assert_eq!(results[0], vec![]);
    }

    #[test]
    fn drops_odd_packets() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9]);

            let (mut even_runnables, mut even_egressors) = even_link(packet_generator);

            let (mut drop_runnables, mut drop_egressors) = DropLink::new()
                .ingressor(even_egressors.pop().unwrap())
                .build_link();

            even_runnables.append(&mut drop_runnables);

            let link = (
                even_runnables,
                vec![even_egressors.pop().unwrap(), drop_egressors.remove(0)],
            );
            run_link(link).await
        });
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
    }

    #[test]
    fn drops_randomly() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link: Link<i32> = DropLink::new()
                .ingressor(immediate_stream(packets))
                .drop_chance(0.7)
                .seed(0)
                .build_link();

            run_link(link).await
        });
        assert_eq!(results[0], vec![1, 2, 1337, 7]);
    }
}
