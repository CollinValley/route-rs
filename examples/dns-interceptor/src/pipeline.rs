// Generated by route-rs-graphgen
// Source graph: examples/dns-interceptor/src/pipeline.xml

use crate::elements::*;
use crate::packets::*;
use futures::lazy;
use route_rs_runtime::link::*;
use route_rs_runtime::pipeline::{InputChannelLink, OutputChannelLink};

pub struct Pipeline {}

impl route_rs_runtime::pipeline::Runner for Pipeline {
    type Input = (Interface, SimplePacket);
    type Output = (Interface, SimplePacket);

    fn run(
        input_channel: crossbeam::Receiver<Self::Input>,
        output_channel: crossbeam::Sender<Self::Output>,
    ) {
        let elem_1_setinterfacebydestination = SetInterfaceByDestination::new();
        let elem_2_classifydns = ClassifyDNS::new();
        let elem_3_localdnsinterceptor = LocalDNSInterceptor::new();

        let link_1 = InputChannelLink::new(input_channel);

        let link_2 = ProcessLink::new(Box::new(link_1), elem_1_setinterfacebydestination);

        let link_3 = ClassifyLink::new(
            Box::new(link_2),
            elem_2_classifydns,
            Box::new(|c| match c {
                ClassifyDNSOutput::DNS => 0,
                _ => 1,
            }),
            10,
            2,
        );
        let link_3_ingressor = link_3.ingressor;
        let mut link_3_egressors = link_3.egressors.into_iter();
        let link_3_egressor_0 = link_3_egressors.next().unwrap();
        let link_3_egressor_1 = link_3_egressors.next().unwrap();

        let link_4 = ProcessLink::new(Box::new(link_3_egressor_0), elem_3_localdnsinterceptor);

        let link_5 = JoinLink::new(vec![Box::new(link_4), Box::new(link_3_egressor_1)], 10);
        let link_5_egressor = link_5.egressor;
        let mut link_5_ingressors = link_5.ingressors.into_iter();
        let link_5_ingressor_0 = link_5_ingressors.next().unwrap();
        let link_5_ingressor_1 = link_5_ingressors.next().unwrap();

        let link_6 = OutputChannelLink::new(Box::new(link_5_egressor), output_channel);

        tokio::run(lazy(move || {
            tokio::spawn(link_3_ingressor);
            tokio::spawn(link_5_ingressor_0);
            tokio::spawn(link_5_ingressor_1);
            tokio::spawn(link_6);
            Ok(())
        }));
    }
}
