use crate::link::{EgressLinkBuilder, IngressLinkBuilder, Link, LinkBuilder, TokioRunnable};
use crate::utils::test::packet_collectors::ExhaustiveCollector;
use crossbeam::crossbeam_channel;
use std::fmt::Debug;
use tokio::runtime;
use tokio::task::JoinHandle;

/// Runner is a user facing helper function for running the constructed router.
///
/// Its only argument is a function pointer that takes no arguments and returns a Link type. This
/// master link should contain all the runnables and outputs of the router, which in turn allows
/// this function to initialize and start the Router.
///
/// In general, the `Link` returned by the router should contain only TokioRunnables and no PacketStreams,
/// since production routers are self contained with all their output going to links that push the packets
/// out the routers physical ports.
///
/// However, if the link your `link_builder()` fn provides does return egressors, this function will automatically
/// hook them into `PacketCollector` links, and whatever packets come out of those egressors will be returned to you once
/// the router completes operation and joins. In a production router, the router likely never stops running so
/// nothing will ever get returned.  Use this functionality only for testing.  
pub fn runner<OutputPacket: Debug + Send + Clone + 'static>(
    link_builder: fn() -> Link<OutputPacket>,
) -> Vec<Vec<OutputPacket>> {
    let mut runtime = runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let (mut runnables, egressors) = link_builder();

        let (mut consumers, receivers): (
            Vec<TokioRunnable>,
            Vec<crossbeam_channel::Receiver<OutputPacket>>,
        ) = egressors
            .into_iter()
            .map(|egressor| {
                let (s, r) = crossbeam_channel::unbounded::<OutputPacket>();
                // TODO: Do we care about consumer IDs? Are they helpful to debug test examples?
                let consumer: TokioRunnable = Box::new(ExhaustiveCollector::new(0, egressor, s));
                (consumer, r)
            })
            .unzip();

        runnables.append(&mut consumers);

        let handles: Vec<JoinHandle<()>> = runnables.into_iter().map(tokio::spawn).collect();
        // 🏃💨💨
        for handle in handles {
            handle.await.unwrap();
        }

        receivers
            .into_iter()
            .map(|receiver| receiver.iter().collect())
            .collect()
    })
}


/// `build_and_run_router` is designed to assemble ingress and egress links for integrating to a
/// variety of I/O implementations (e.g. af_packet, pcap, etc). This allows us to generically
/// hook up a router for testing purposes without changing the internals.
pub fn build_and_run_router<
    IngressPacket,
    IngressLink: IngressLinkBuilder<IngressPacket>,
    EgressPacket,
    EgressLink: EgressLinkBuilder<EgressPacket>,
    Router: LinkBuilder<IngressPacket, EgressPacket>,
>(
    ingress_receivers: Vec<IngressLink::Receiver>,
    egress_senders: Vec<EgressLink::Sender>,
    router: Router,
) {
    let mut runtime = runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        // Create Ingress Links from the receivers
        let ingressors = ingress_receivers
            .into_iter()
            .map(|r| IngressLink::new().channel(r).build_link())
            .collect::<Vec<Link<IngressPacket>>>();

        // Collapse runnables and egressors so that we can pass them along
        let mut ingress_runnables = vec![];
        let mut ingress_egressors = vec![];
        ingressors
            .into_iter()
            .for_each(|(mut runnables, mut egressors)| {
                ingress_runnables.append(&mut runnables);
                ingress_egressors.append(&mut egressors);
            });

        // Set up the actual business logic router
        let (mut router_runnables, router_egressors) =
            router.ingressors(ingress_egressors).build_link();

        // We know there should be a 1:1 correspondence between egressors from the router and senders,
        // so we can zip them together and then build the egress links
        let egressors = router_egressors
            .into_iter()
            .zip(egress_senders.into_iter())
            .map(|(egressor, sender)| {
                EgressLink::new()
                    .ingressor(egressor)
                    .channel(sender)
                    .build_link()
            })
            .collect::<Vec<Link<()>>>();

        // Egress links don't have any egressors, so all we need to do is pick up the runnables
        let mut egress_runnables = vec![];
        egressors
            .into_iter()
            .for_each(|(mut runnables, _egressors)| {
                egress_runnables.append(&mut runnables);
            });

        let mut all_runnables = vec![];
        all_runnables.append(&mut ingress_runnables);
        all_runnables.append(&mut router_runnables);
        all_runnables.append(&mut egress_runnables);

        let handles: Vec<JoinHandle<()>> = all_runnables.into_iter().map(tokio::spawn).collect();
        // 🏃💨💨
        for handle in handles {
            handle.await.unwrap();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::primitive::{ProcessLink, InputChannelLink, OutputChannelLink};
    use crate::link::ProcessLinkBuilder;
    use crate::processor::Identity;
    use crate::link::composite::MtoNLink;

    #[test]
    fn identity_router() {
        let router = ProcessLink::new()
            .processor(Identity::<u32>::new());

        let (ingress_sender, ingress_receiver) = crossbeam::unbounded::<u32>();
        let (egress_sender, egress_receiver) = crossbeam::unbounded::<u32>();

        assert_eq!(ingress_sender.send(1234), Ok(()));
        assert_eq!(ingress_sender.send(5678), Ok(()));

        drop(ingress_sender);

        // TODO: Remove this comment if we fix the ugliness or decide it's intractable
        // This is the ugliest part of this implementation and I would love to figure out a way to
        // make it less awful. Named parameters don't seem to work here for mysterious reasons.
        build_and_run_router::<_, InputChannelLink<u32>, _, OutputChannelLink<u32>, _>(
            vec![ingress_receiver],
            vec![egress_sender],
            router,
        );

        assert_eq!(egress_receiver.recv(), Ok(1234));
        assert_eq!(egress_receiver.recv(), Ok(5678));
    }

    #[test]
    fn mton_router() {
        let router = MtoNLink::new()
            .num_egressors(3);

        let (ingress_sender_1, ingress_receiver_1) = crossbeam::unbounded::<u32>();
        let (ingress_sender_2, ingress_receiver_2) = crossbeam::unbounded::<u32>();

        let (egress_sender_1, egress_receiver_1) = crossbeam::unbounded::<u32>();
        let (egress_sender_2, egress_receiver_2) = crossbeam::unbounded::<u32>();
        let (egress_sender_3, egress_receiver_3) = crossbeam::unbounded::<u32>();

        assert_eq!(ingress_sender_1.send(1234), Ok(()));
        assert_eq!(ingress_sender_2.send(5678), Ok(()));

        drop(ingress_sender_1);
        drop(ingress_sender_2);

        build_and_run_router::<_, InputChannelLink<u32>, _, OutputChannelLink<u32>, _>(
            vec![ingress_receiver_1, ingress_receiver_2],
            vec![egress_sender_1, egress_sender_2, egress_sender_3],
            router,
        );

        let mut egress_1_items = egress_receiver_1.iter().collect::<Vec<u32>>();
        egress_1_items.sort();
        assert_eq!(egress_1_items, &[1234, 5678]);
        let mut egress_2_items = egress_receiver_2.iter().collect::<Vec<u32>>();
        egress_2_items.sort();
        assert_eq!(egress_2_items, &[1234, 5678]);
        let mut egress_3_items = egress_receiver_3.iter().collect::<Vec<u32>>();
        egress_3_items.sort();
        assert_eq!(egress_3_items, &[1234, 5678]);
    }
}