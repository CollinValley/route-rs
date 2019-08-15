use crate::packets::IntegerPacket;
use futures::lazy;
use route_rs_runtime::element::IdentityElement;
use route_rs_runtime::link::ElementLink;
use route_rs_runtime::pipeline::{InputChannelLink, OutputChannelLink};

pub struct Pipeline {}

impl route_rs_runtime::pipeline::Runner for Pipeline {
    type Input = IntegerPacket;
    type Output = IntegerPacket;

    fn run(
        input_channel: crossbeam::Receiver<Self::Input>,
        output_channel: crossbeam::Sender<Self::Output>,
    ) {
        let elem_1 = IdentityElement::new();

        let link_1 = InputChannelLink::new(input_channel);
        let link_2 = ElementLink::new(Box::new(link_1), elem_1);
        let link_3 = OutputChannelLink::new(Box::new(link_2), output_channel);

        tokio::run(lazy(|| {
            tokio::spawn(link_3);
            Ok(())
        }));
    }
}
