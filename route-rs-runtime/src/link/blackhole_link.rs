use crate::link::{Link, PacketStream, TokioRunnable};
use futures::{Async, Future, Poll};

/// Link that drops all packets ingressed.
#[derive(Default)]
pub struct BlackHoleLink<Packet: Sized> {
    hole: Option<BlackHole<Packet>>,
}

impl<Packet> BlackHoleLink<Packet> {
    pub fn new() -> Self {
        BlackHoleLink { hole: None }
    }
}

impl<Packet: Sized + 'static> Link<Packet, ()> for BlackHoleLink<Packet> {
    fn ingressors(&self, ingress_streams: Vec<PacketStream<Packet>>) -> Self {
        BlackHoleLink {
            hole: Some(BlackHole::new(ingress_streams)),
        }
    }

    fn build_link(self) -> (Vec<TokioRunnable>, Vec<PacketStream<()>>) {
        if self.hole.is_none() {
            panic!("Cannot build link! Missing ingress streams")
        }

        (vec![Box::new(self.hole.unwrap())], vec![])
    }
}

pub struct BlackHole<Packet> {
    ingress_streams: Vec<PacketStream<Packet>>,
}

impl<Packet: Sized> BlackHole<Packet> {
    fn new(ingress_streams: Vec<PacketStream<Packet>>) -> Self {
        BlackHole { ingress_streams }
    }
}

impl<Packet: Sized> Future for BlackHole<Packet> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            for ingress_stream in self.ingress_streams.iter_mut() {
                if try_ready!(ingress_stream.poll()).is_none() {
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::element::Classifier;
    use crate::link::ClassifyLink;
    use crate::utils::test::packet_collectors::ExhaustiveCollector;
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use core::time;
    use crossbeam::crossbeam_channel;
    use futures::future::lazy;

    struct ClassifyEvenness {}

    impl ClassifyEvenness {
        pub fn new() -> Self {
            ClassifyEvenness {}
        }
    }

    impl Classifier for ClassifyEvenness {
        type Packet = i32;
        type Class = bool;

        fn classify(&self, packet: &Self::Packet) -> Self::Class {
            packet % 2 == 0
        }
    }

    fn run_tokio(runnables: Vec<TokioRunnable>) {
        tokio::run(lazy(|| {
            for runnable in runnables {
                tokio::spawn(runnable);
            }
            Ok(())
        }));
    }

    #[test]
    #[should_panic]
    fn panics_if_improperly_built() {
        BlackHoleLink::<i32>::new().build_link();
    }

    #[test]
    fn finishes() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];
        let packet_generator = immediate_stream(packets.clone());

        let (runnables, _) = BlackHoleLink::new()
            .ingressors(vec![Box::new(packet_generator)])
            .build_link();

        run_tokio(runnables);

        //In this test, we just ensure that it finishes.
    }

    #[test]
    fn finishes_with_wait() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];
        let packet_generator = PacketIntervalGenerator::new(
            time::Duration::from_millis(10),
            packets.clone().into_iter(),
        );

        let (runnables, _) = BlackHoleLink::new()
            .ingressors(vec![Box::new(packet_generator)])
            .build_link();

        run_tokio(runnables);

        //In this test, we just ensure that it finishes.
    }

    #[test]
    fn odd_packets() {
        let default_channel_size = 10;
        let number_branches = 2;
        let packet_generator = immediate_stream(vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9]);

        let elem0 = ClassifyEvenness::new();

        let mut link0 = ClassifyLink::new(
            packet_generator,
            elem0,
            Box::new(|evenness| if evenness { 0 } else { 1 }),
            default_channel_size,
            number_branches,
        );

        let drain0 = link0.ingressor;

        let (mut black_hole_runnables, _) = BlackHoleLink::new()
            .ingressors(vec![Box::new(link0.egressors.pop().unwrap())])
            .build_link();

        let (s0, link0_port0_collector_output) = crossbeam_channel::unbounded();
        let link0_port0_collector =
            ExhaustiveCollector::new(0, Box::new(link0.egressors.pop().unwrap()), s0);

        let mut runnables: Vec<TokioRunnable> = Vec::new();
        runnables.push(Box::new(drain0));
        runnables.push(Box::new(link0_port0_collector));
        runnables.append(&mut black_hole_runnables);

        run_tokio(runnables);

        let elem0_port0_output: Vec<_> = link0_port0_collector_output.iter().collect();
        assert_eq!(elem0_port0_output, vec![0, 2, 420, 4, 6, 8]);
    }
}
