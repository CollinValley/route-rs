use futures::{Future, Stream, Async, Poll, task};
use crossbeam::crossbeam_channel::{Sender, Receiver, TryRecvError};
use crossbeam::crossbeam_channel;
use crate::api::ElementStream;

pub trait AsyncElement {
    type Input: Sized;
    type Output: Sized;

    fn process(&mut self, packet: Self::Input) -> Self::Output;
}

/// The AsyncElementLink is a wrapper to create and contain both sides of the
/// link, the consumer, which intakes and processes packets, and the provider,
/// which provides an interface where the next element retrieves the output
/// packet.
pub struct AsyncElementLink< E: AsyncElement> {
    pub consumer: AsyncElementConsumer<E>,
    pub provider: AsyncElementProvider<E>,
    pub overseer: AsyncElementOverseer<E>
}

impl<E: AsyncElement> AsyncElementLink<E> {
    pub fn new(input_stream: ElementStream<E::Input>, element: E, queue_capacity: usize) -> Self {
        let (to_provider, from_consumer) = crossbeam_channel::bounded::<Option<E::Output>>(queue_capacity);
        let (await_consumer, wake_provider) = crossbeam_channel::bounded::<task::Task>(1);
        let (await_provider, wake_consumer) = crossbeam_channel::bounded::<task::Task>(1);

        AsyncElementLink {
            consumer: AsyncElementConsumer::new(input_stream, 
                                                to_provider, 
                                                element, 
                                                await_provider.clone(), 
                                                wake_provider.clone()),

            provider: AsyncElementProvider::new(from_consumer.clone(),
                                                await_consumer.clone(),
                                                wake_consumer.clone()),

            overseer: AsyncElementOverseer::new(from_consumer, 
                                                wake_provider, 
                                                wake_consumer)
        }
    }
}

pub struct AsyncElementOverseer<E: AsyncElement> {
    from_consumer: Receiver<Option<E::Output>>,
    wake_provider: Receiver<task::Task>,
    wake_consumer: Receiver<task::Task>
}

impl<E: AsyncElement> AsyncElementOverseer<E> {
    fn new(
        from_consumer: Receiver<Option<E::Output>>,
        wake_provider: Receiver<task::Task>,
        wake_consumer: Receiver<task::Task>        
    ) -> Self {
        AsyncElementOverseer {
            from_consumer,
            wake_provider,
            wake_consumer
        }
    }
}

impl<E: AsyncElement> Future for AsyncElementOverseer<E> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.from_consumer.is_empty(){
            match self.wake_consumer.try_recv() {
                Ok(task) => {
                    task.notify(); 
                },
                Err(TryRecvError::Empty) => { },
                Err(TryRecvError::Disconnected) => { 
                    return Ok(Async::Ready(()));
                }
            }
        } else {
            match self.wake_provider.try_recv() {
                Ok(task) => {
                    task.notify(); 
                },
                Err(_) => { },         
            }
        }
        task::current().notify();
        Ok(Async::NotReady)
    }
}

/// The AsyncElementConsumer is responsible for polling its input stream,
/// processing them using the `element`s process function, and pushing the
/// output packet onto the to_provider queue. It does work in batches, so it
/// will continue to pull packets as long as it can make forward progess,
/// after which it will return NotReady to sleep. This is handed to, and is
/// polled by the runtime.
pub struct AsyncElementConsumer<E: AsyncElement> {
    input_stream: ElementStream<E::Input>,
    to_provider: Sender<Option<E::Output>>,
    element: E,
    await_provider: Sender<task::Task>,
    wake_provider: Receiver<task::Task>
}

impl<E: AsyncElement> AsyncElementConsumer<E> {
    fn new(
        input_stream: ElementStream<E::Input>, 
        to_provider: Sender<Option<E::Output>>, 
        element: E,
        await_provider: Sender<task::Task>,
        wake_provider: Receiver<task::Task>) 
    -> Self {
        AsyncElementConsumer {
            input_stream,
            to_provider,
            element,
            await_provider,
            wake_provider
        }
    }
}

impl<E: AsyncElement> Drop for AsyncElementConsumer<E> {
    fn drop(&mut self) {
        if let Err(err) = self.to_provider.try_send(None) {
            panic!("Consumer: Drop: try_send to_provider, fail?: {:?}", err);
        }
        if let Ok(task) = self.wake_provider.try_recv() {
            task.notify();
        } 
    }
}

impl<E: AsyncElement> Future for AsyncElementConsumer<E> {
    type Item = ();
    type Error = ();

    /// Implement Poll for Future for AsyncElementConsumer
    /// 
    /// Note that this function works a bit different, it continues to process
    /// packets off it's input queue until it reaches a point where it can not
    /// make forward progress. There are three cases:
    /// ###
    /// #1 The to_provider queue is full, we notify the provider that we need
    /// awaking when there is work to do, and go to sleep.
    /// 
    /// #2 The input_stream returns a NotReady, we sleep, with the assumption
    /// that whomever produced the NotReady will awaken the task in the Future.
    /// 
    /// #3 We get a Ready(None), in which case we push a None onto the to_provider
    /// queue and then return Ready(()), which means we enter tear-down, since there
    /// is no futher work to complete.
    /// ###
    /// By Sleep, we mean we return a NotReady to the runtime which will sleep the task.
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop{
            if self.to_provider.is_full() {
                let task = task::current();
                if let Err(_) = self.await_provider.try_send(task) {
                    task::current().notify();
                }
                return Ok(Async::NotReady)
            }
            let input_packet_option: Option<E::Input> = try_ready!(self.input_stream.poll());

            match input_packet_option {
                None => {
                    return Ok(Async::Ready(()))
                },
                Some(input_packet) => {
                    let output_packet: E::Output = self.element.process(input_packet);
                    if let Err(err) = self.to_provider.try_send(Some(output_packet)) {
                        panic!("Error in to_provider sender, have nowhere to put packet: {:?}", err);
                    }
                    if let Ok(task) = self.wake_provider.try_recv() {
                        task.notify();
                    }
                }
            }
        }
    }
}

/// The Provider side of the AsyncElement is responsible to converting the
/// output queue of processed packets, which is a crossbeam channel, to a 
/// Stream that can be polled for packets. It ends up being owned by the 
/// element which is polling for packets. 
pub struct AsyncElementProvider<E: AsyncElement> {
    from_consumer: Receiver<Option<E::Output>>,
    await_consumer: Sender<task::Task>,
    wake_consumer: Receiver<task::Task>
}

impl<E: AsyncElement> AsyncElementProvider<E> {
    fn new(
        from_consumer: Receiver<Option<E::Output>>, 
        await_consumer: Sender<task::Task>, 
        wake_consumer: Receiver<task::Task>
        ) -> Self {
            AsyncElementProvider {
                from_consumer,
                await_consumer,
                wake_consumer
            }
    }
}

impl<E: AsyncElement> Drop for AsyncElementProvider<E> {
    fn drop(&mut self) {
        if let Ok(task) = self.wake_consumer.try_recv() {
            task.notify();
        }
    }
}

impl<E: AsyncElement> Stream for AsyncElementProvider<E> {
    type Item = E::Output;
    type Error = ();

    ///Implement Poll for Stream for AsyncElementProvider
    /// 
    /// This function, tries to retrieve a packet off the `from_consumer`
    /// channel, there are four cases: 
    /// ###
    /// #1 Ok(Some(Packet)): Got a packet.if the consumer needs (likely due to 
    /// an until now full channel) to be awoken, wake them. Return the Async::Ready(Option(Packet))
    /// 
    /// #2 Ok(None): this means that the consumer is in tear-down, and we
    /// will no longer be receivig packets. Return Async::Ready(None) to forward propagate teardown
    /// 
    /// #3 Err(TryRecvError::Empty): Packet queue is empty, await the consumer to awaken us with more
    /// work, and return Async::NotReady to signal to runtime to sleep this task.
    /// 
    /// #4 Err(TryRecvError::Disconnected): Consumer is in teardown and has dropped its side of the
    /// from_consumer channel; we will no longer receive packets. Return Async::Ready(None) to forward
    /// propagate teardown.
    /// ###
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.from_consumer.try_recv() {
            Ok(Some(packet)) => {
                if let Ok(task) = self.wake_consumer.try_recv() {
                        task.notify();
                }
                Ok(Async::Ready(Some(packet)))
            },
            Ok(None) => {
                Ok(Async::Ready(None))
            },
            Err(TryRecvError::Empty) => {
                let task = task::current();
                if let Err(_) = self.await_consumer.try_send(task) {
                    task::current().notify();
                }
                Ok(Async::NotReady)
            },
            Err(TryRecvError::Disconnected) => {
                Ok(Async::Ready(None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::packet_generators::{PacketIntervalGenerator, immediate_stream};
    use crate::utils::test::packet_collectors::ExhaustiveCollector;
    use crate::api::element::{Element, ElementLink};
    use core::time;
    use futures::future::lazy;

    #[allow(dead_code)]
    struct IdentityElement {
        id: i32
    }

    impl Element for IdentityElement {
        type Input = i32;
        type Output = i32;

        fn process(&mut self, packet: Self::Input) -> Self::Output {
            packet
        }
    }

   #[allow(dead_code)]
    struct AsyncIdentityElement {
        id: i32
    }

    impl AsyncElement for AsyncIdentityElement {
        type Input = i32;
        type Output = i32;

        fn process(&mut self, packet: Self::Input) -> Self::Output {
            packet
        }
    }

    #[test]
    fn one_async_element() {
        let default_channel_size = 10;
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8 , 9];
        let packet_generator = PacketIntervalGenerator::new(time::Duration::from_millis(100), packets.clone().into_iter());

        let elem0 = AsyncIdentityElement { id: 0 };

        let elem0_link = AsyncElementLink::new(Box::new(packet_generator), elem0, default_channel_size);

        let (s, r) = crossbeam_channel::unbounded();
        let elem0_drain = elem0_link.consumer;
        let elem0_collector = ExhaustiveCollector::new(0, Box::new(elem0_link.provider), s);
        let elem0_overseer = elem0_link.overseer;

        tokio::run(lazy (|| {
            tokio::spawn(elem0_drain);
            tokio::spawn(elem0_collector);
            tokio::spawn(elem0_overseer);
            Ok(())
        }));

        let router_output: Vec<_> = r.iter().collect();
        assert_eq!(router_output, packets);
    }

    #[test]
    fn one_async_element_long_stream() {
        let default_channel_size = 10;
        let packet_generator = immediate_stream(0..=2000);

        let elem0 = AsyncIdentityElement { id: 0 };

        let elem0_link = AsyncElementLink::new(Box::new(packet_generator), elem0, default_channel_size);

        let (s, r) = crossbeam_channel::unbounded();
        let elem0_drain = elem0_link.consumer;
        let elem0_collector = ExhaustiveCollector::new(0, Box::new(elem0_link.provider), s);
        let elem0_overseer = elem0_link.overseer;

        tokio::run(lazy (|| {
            tokio::spawn(elem0_drain);
            tokio::spawn(elem0_collector);
            tokio::spawn(elem0_overseer);
            Ok(())
        }));

        let router_output: Vec<_> = r.iter().collect();
        assert_eq!(router_output.len(), 2001);
    }

    #[test]
    fn two_async_elements() {
        let default_channel_size = 10;
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8 ,9];
        let packet_generator = PacketIntervalGenerator::new(time::Duration::from_millis(100), packets.clone().into_iter());

        let elem0 = AsyncIdentityElement { id: 0 };
        let elem1 = AsyncIdentityElement { id: 1 };

        let elem0_link = AsyncElementLink::new(Box::new(packet_generator), elem0, default_channel_size);
        let elem1_link = AsyncElementLink::new(Box::new(elem0_link.provider), elem1, default_channel_size);

        let elem0_drain = elem0_link.consumer;
        let elem1_drain = elem1_link.consumer;

        let (s, r) = crossbeam_channel::unbounded();
        let elem1_collector = ExhaustiveCollector::new(0, Box::new(elem1_link.provider), s);

        tokio::run(lazy (|| {
            tokio::spawn(elem0_drain);
            tokio::spawn(elem1_drain);
            tokio::spawn(elem1_collector);
            Ok(())
        }));

        let router_output: Vec<_> = r.iter().collect();
        assert_eq!(router_output, packets);
    }

    #[test]
    fn series_sync_and_async() {
        let default_channel_size = 10;
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];
        let packet_generator = PacketIntervalGenerator::new(time::Duration::from_millis(100), packets.clone().into_iter());

        let elem0 = IdentityElement { id: 0 };
        let elem1 = AsyncIdentityElement { id: 1 };
        let elem2 = IdentityElement { id: 2 };
        let elem3 = AsyncIdentityElement { id: 3 };

        let elem0_link = ElementLink::new(Box::new(packet_generator), elem0);
        let elem1_link = AsyncElementLink::new(Box::new(elem0_link), elem1, default_channel_size);
        let elem2_link = ElementLink::new(Box::new(elem1_link.provider), elem2);
        let elem3_link = AsyncElementLink::new(Box::new(elem2_link), elem3, default_channel_size);

        let elem1_drain = elem1_link.consumer;
        let elem3_drain = elem3_link.consumer;

        let (s, r) = crossbeam_channel::unbounded();
        let elem3_collector = ExhaustiveCollector::new(0, Box::new(elem3_link.provider), s);

        let elem1_overseer = elem1_link.overseer;
        let elem3_overseer = elem3_link.overseer;

        tokio::run(lazy (|| {
            tokio::spawn(elem1_drain);
            tokio::spawn(elem3_drain); 
            tokio::spawn(elem3_collector);
            tokio::spawn(elem1_overseer);
            tokio::spawn(elem3_overseer);
            Ok(())
        }));

        let router_output: Vec<_> = r.iter().collect();
        assert_eq!(router_output, packets);
    }
}