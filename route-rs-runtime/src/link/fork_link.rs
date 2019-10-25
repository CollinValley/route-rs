use crate::link::task_park::*;
use crate::link::{Link, LinkBuilder, PacketStream, QueueEgressor};
use crossbeam::atomic::AtomicCell;
use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::{Receiver, Sender};
use futures::{Async, Future, Poll, Stream};
use std::sync::Arc;

#[derive(Default)]
pub struct ForkLink<Packet: Clone + Send> {
    in_stream: Option<PacketStream<Packet>>,
    queue_capacity: usize,
    num_egressors: Option<usize>,
}

impl<Packet: Clone + Send> ForkLink<Packet> {
    pub fn new() -> Self {
        ForkLink {
            in_stream: None,
            queue_capacity: 10,
            num_egressors: None,
        }
    }

    /// Changes queue_capacity, default value is 10.
    /// Valid range is 1..=1000
    pub fn queue_capacity(self, queue_capacity: usize) -> Self {
        assert!(
            (1..=1000).contains(&queue_capacity),
            format!(
                "queue_capacity: {}, must be in range 1..=1000",
                queue_capacity
            )
        );

        ForkLink {
            in_stream: self.in_stream,
            queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    pub fn num_egressors(self, num_egressors: usize) -> Self {
        assert!(
            (1..=1000).contains(&num_egressors),
            format!(
                "num_egressors: {}, must be in range 1..=1000",
                num_egressors
            )
        );

        ForkLink {
            in_stream: self.in_stream,
            queue_capacity: self.queue_capacity,
            num_egressors: Some(num_egressors),
        }
    }

    pub fn ingressor(self, in_stream: PacketStream<Packet>) -> Self {
        ForkLink {
            in_stream: Some(in_stream),
            queue_capacity: self.queue_capacity,
            num_egressors: self.num_egressors,
        }
    }
}

impl<Packet: Send + Clone + 'static> LinkBuilder<Packet, Packet> for ForkLink<Packet> {
    fn ingressors(self, mut in_streams: Vec<PacketStream<Packet>>) -> Self {
        assert_eq!(
            in_streams.len(),
            1,
            "ForkLinks may only take one input stream!"
        );
        ForkLink {
            in_stream: Some(in_streams.remove(0)),
            queue_capacity: self.queue_capacity,
            num_egressors: self.num_egressors,
        }
    }

    fn build_link(self) -> Link<Packet> {
        if self.in_stream.is_none() {
            panic!("Cannot build link! Missing input stream");
        } else if self.num_egressors.is_none() {
            panic!("Cannot build link! Missing number of num_egressors");
        } else {
            let mut to_egressors: Vec<Sender<Option<Packet>>> = Vec::new();
            let mut egressors: Vec<PacketStream<Packet>> = Vec::new();

            let mut from_ingressors: Vec<Receiver<Option<Packet>>> = Vec::new();

            let mut task_parks: Vec<Arc<AtomicCell<TaskParkState>>> = Vec::new();

            for _ in 0..self.num_egressors.unwrap() {
                let (to_egressor, from_ingressor) =
                    crossbeam_channel::bounded::<Option<Packet>>(self.queue_capacity);
                let task_park = Arc::new(AtomicCell::new(TaskParkState::Empty));

                let egressor = QueueEgressor::new(from_ingressor.clone(), Arc::clone(&task_park));

                to_egressors.push(to_egressor);
                egressors.push(Box::new(egressor));
                from_ingressors.push(from_ingressor);
                task_parks.push(task_park);
            }

            let ingressor = ForkIngressor::new(self.in_stream.unwrap(), to_egressors, task_parks);

            (vec![Box::new(ingressor)], egressors)
        }
    }
}

pub struct ForkIngressor<P> {
    input_stream: PacketStream<P>,
    to_egressors: Vec<Sender<Option<P>>>,
    task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
}

impl<P> ForkIngressor<P> {
    fn new(
        input_stream: PacketStream<P>,
        to_egressors: Vec<Sender<Option<P>>>,
        task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
    ) -> Self {
        ForkIngressor {
            input_stream,
            to_egressors,
            task_parks,
        }
    }
}

impl<P> Drop for ForkIngressor<P> {
    fn drop(&mut self) {
        //TODO: do this with a closure or something, this could be a one-liner
        for to_egressor in self.to_egressors.iter() {
            if let Err(err) = to_egressor.try_send(None) {
                panic!("Ingressor: Drop: try_send to egressor, fail?: {:?}", err);
            }
        }
        for task_park in self.task_parks.iter() {
            die_and_notify(&task_park);
        }
    }
}

impl<P: Send + Clone> Future for ForkIngressor<P> {
    type Item = ();
    type Error = ();

    /// If any of the channels are full, we await that channel to clear before processing a new packet.
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            for (port, to_egressor) in self.to_egressors.iter().enumerate() {
                if to_egressor.is_full() {
                    park_and_notify(&self.task_parks[port]);
                    return Ok(Async::NotReady);
                }
            }
            let packet_option: Option<P> = try_ready!(self.input_stream.poll());

            match packet_option {
                None => return Ok(Async::Ready(())),
                Some(packet) => {
                    //TODO: should packet but put in an iterator? or only cloned? or last one reused?
                    assert!(self.to_egressors.len() == self.task_parks.len());
                    for port in 0..self.to_egressors.len() {
                        if let Err(err) = self.to_egressors[port].try_send(Some(packet.clone())) {
                            panic!(
                                "Error in to_egressors[{}] sender, have nowhere to put packet: {:?}",
                                port, err
                            );
                        }
                        unpark_and_notify(&self.task_parks[port]);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::harness::run_link;
    use crate::utils::test::packet_generators::immediate_stream;

    #[test]
    #[should_panic]
    fn panics_when_built_without_input_streams() {
        ForkLink::<i32>::new().num_egressors(10).build_link();
    }

    #[test]
    #[should_panic]
    fn panics_when_built_without_num_egressors() {
        ForkLink::<i32>::new()
            .ingressors(vec![immediate_stream(vec![])])
            .build_link();
    }

    #[test]
    fn builder_methods_work_in_any_order() {
        ForkLink::<i32>::new()
            .ingressor(immediate_stream(vec![]))
            .num_egressors(2)
            .build_link();

        ForkLink::<i32>::new()
            .num_egressors(2)
            .ingressor(immediate_stream(vec![]))
            .build_link();
    }

    #[test]
    fn no_input() {
        let link = ForkLink::<i32>::new()
            .ingressor(immediate_stream(vec![]))
            .num_egressors(1)
            .build_link();

        let results = run_link(link);
        assert!(results[0].is_empty());
    }

    #[test]
    fn one_way() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let link = ForkLink::new()
            .ingressor(immediate_stream(packets.clone()))
            .num_egressors(1)
            .build_link();

        let results = run_link(link);
        assert_eq!(results[0], packets);
    }

    #[test]
    fn two_way() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let link = ForkLink::new()
            .ingressor(immediate_stream(packets.clone()))
            .num_egressors(2)
            .build_link();

        let results = run_link(link);
        assert_eq!(results[0], packets.clone());
        assert_eq!(results[1], packets);
    }

    #[test]
    fn three_way() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let link = ForkLink::new()
            .ingressor(immediate_stream(packets.clone()))
            .num_egressors(3)
            .build_link();

        let results = run_link(link);
        assert_eq!(results[0], packets.clone());
        assert_eq!(results[1], packets.clone());
        assert_eq!(results[2], packets);
    }
}