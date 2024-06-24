use route_rs_packets::EthernetFrame;
use route_rs_runtime::link::{
    primitive::{ClassifyLink, JoinLink, ProcessLink},
    Link, LinkBuilder, PacketStream, ProcessLinkBuilder,
};
use route_rs_runtime::utils::{runner::runner, test::packet_generators::immediate_stream};

mod classifiers;
mod processors;

fn main() {
    let data_v4: Vec<u8> = vec![
        0xde, 0xad, 0xbe, 0xef, 0xff, 0xff, 1, 2, 3, 4, 5, 6, 8, 00, 0x45, 0, 0, 20, 0, 0, 0, 0,
        64, 17, 0, 0, 192, 178, 128, 0, 10, 0, 0, 1,
    ];
    let data_v6: Vec<u8> = vec![
        0xde, 0xad, 0xbe, 0xef, 0xff, 0xff, 1, 2, 3, 4, 5, 6, 0x86, 0xDD, 0x60, 0, 0, 0, 0, 4, 17,
        64, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad,
        0xbe, 0xef, 0x20, 0x01, 0x0d, 0xb8, 0xbe, 0xef, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xa, 0xb,
        0xc, 0xd,
    ];
    let test_frame1 = EthernetFrame::from_buffer(data_v4, 0).unwrap();
    let test_frame2 = EthernetFrame::from_buffer(data_v6, 0).unwrap();

    let results = runner(router_runner);
    println!("It finished!");

    assert_eq!(results[1][0], test_frame1);
    assert_eq!(results[2][0], test_frame2);
    println!("Got all packets on the expected interface!");
}

fn router_runner() -> Link<EthernetFrame> {
    let data_v4: Vec<u8> = vec![
        0xde, 0xad, 0xbe, 0xef, 0xff, 0xff, 1, 2, 3, 4, 5, 6, 8, 00, 0x45, 0, 0, 20, 0, 0, 0, 0,
        64, 17, 0, 0, 192, 178, 128, 0, 10, 0, 0, 1,
    ];
    let data_v6: Vec<u8> = vec![
        0xde, 0xad, 0xbe, 0xef, 0xff, 0xff, 1, 2, 3, 4, 5, 6, 0x86, 0xDD, 0x60, 0, 0, 0, 0, 4, 17,
        64, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad,
        0xbe, 0xef, 0x20, 0x01, 0x0d, 0xb8, 0xbe, 0xef, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xa, 0xb,
        0xc, 0xd,
    ];
    let frame1 = EthernetFrame::from_buffer(data_v4, 0).unwrap();
    let frame2 = EthernetFrame::from_buffer(data_v6, 0).unwrap();
    let packets = vec![frame1, frame2];
    // Create our router
    Router::new()
        .ingressors(vec![immediate_stream(packets)])
        .build_link()
}

// Note that Router is not Generic! This router only takes in EthernetFrames
#[derive(Default)]
pub struct Router {
    in_streams: Option<Vec<PacketStream<EthernetFrame>>>,
}

// Then we declare it is a link that take in EthernetFrames and outputs EthernetFrames
// LinkBuilder is always generic, so we need to fill out EthernetFrame
impl LinkBuilder<EthernetFrame, EthernetFrame> for Router {
    fn new() -> Self {
        Router { in_streams: None }
    }

    fn ingressors(self, in_streams: Vec<PacketStream<EthernetFrame>>) -> Self {
        assert!(
            in_streams.len() == 1,
            "Support only one input interface for now"
        );

        if self.in_streams.is_some() {
            panic!("Only support one input interface")
        }

        Router {
            in_streams: Some(in_streams),
        }
    }

    fn ingressor(self, in_stream: PacketStream<EthernetFrame>) -> Self {
        if self.in_streams.is_some() {
            panic!("Only support one input interface")
        }

        Router {
            in_streams: Some(vec![in_stream]),
        }
    }

    fn build_link(self) -> Link<EthernetFrame> {
        if self.in_streams.is_none() {
            panic!("Can not build link, missing input stream");
        } else {
            // TODO: Build the router here

            // Skeleton Concept:                                        /--encap
            //               /--Ipv4Dencap--Ipv4SubnetRouter(Classifier)<--encap      /--Join--Interface 0
            //>--ClassifyIP--<                                          \--encap .... <--Join--Interface 1
            //               \                                          /--encap      \--Join--Interface 2
            //               \--Ipv6Dencap--Ipv6SubnetRouter(Classifier)<--encap
            //                                                          \--encap

            //return an empty thing for now so it compiles.
            let mut all_runnables = vec![];

            let ipv4_router =
                classifiers::Ipv4SubnetRouter::new(classifiers::Interface::Interface0);
            let ipv6_router =
                classifiers::Ipv6SubnetRouter::new(classifiers::Interface::Interface0);

            let (mut classify_runables, mut classify_egressors) = ClassifyLink::new()
                .ingressors(self.in_streams.unwrap())
                .num_egressors(2)
                .classifier(classifiers::ClassifyIP)
                .dispatcher(Box::new(|c| match c {
                    classifiers::ClassifyIPType::IPv4 => 0,
                    classifiers::ClassifyIPType::IPv6 => 1,
                    classifiers::ClassifyIPType::None => 1, // we can't drop packets in a classify. Maybe we do need
                })) // the DropLink back?
                .build_link();
            all_runnables.append(&mut classify_runables);

            //------------Ipv4 Subnet router--------------//

            // Reminder that process links don't have any runnables, so we can ignore that half of the tuple
            let (_, ipv4_dencap_egressors) = ProcessLink::new()
                .ingressor(classify_egressors.remove(0))
                .processor(processors::Ipv4Decap)
                .build_link();

            let (mut ipv4_subnet_router_runnables, mut ipv4_subnet_router_egressors) =
                ClassifyLink::new()
                    .ingressors(ipv4_dencap_egressors)
                    .num_egressors(3)
                    .classifier(ipv4_router)
                    .dispatcher(Box::new(|c| match c {
                        classifiers::Interface::Interface0 => 0,
                        classifiers::Interface::Interface1 => 1,
                        classifiers::Interface::Interface2 => 2,
                    }))
                    .build_link();
            all_runnables.append(&mut ipv4_subnet_router_runnables);

            let (_, mut ipv4_encap_interface0_egressors) = ProcessLink::new()
                .ingressor(ipv4_subnet_router_egressors.remove(0))
                .processor(processors::Ipv4Encap)
                .build_link();

            let (_, mut ipv4_encap_interface1_egressors) = ProcessLink::new()
                .ingressor(ipv4_subnet_router_egressors.remove(0))
                .processor(processors::Ipv4Encap)
                .build_link();

            let (_, mut ipv4_encap_interface2_egressors) = ProcessLink::new()
                .ingressor(ipv4_subnet_router_egressors.remove(0))
                .processor(processors::Ipv4Encap)
                .build_link();

            //----------IPv6 Subnet Router--------------//

            let (_, ipv6_dencap_egressors) = ProcessLink::new()
                .ingressor(classify_egressors.remove(0))
                .processor(processors::Ipv6Decap)
                .build_link();

            let (mut ipv6_subnet_router_runnables, mut ipv6_subnet_router_egressors) =
                ClassifyLink::new()
                    .ingressors(ipv6_dencap_egressors)
                    .num_egressors(3)
                    .classifier(ipv6_router)
                    .dispatcher(Box::new(|c| match c {
                        classifiers::Interface::Interface0 => 0,
                        classifiers::Interface::Interface1 => 1,
                        classifiers::Interface::Interface2 => 2,
                    }))
                    .build_link();
            all_runnables.append(&mut ipv6_subnet_router_runnables);

            let (_, mut ipv6_encap_interface0_egressors) = ProcessLink::new()
                .ingressor(ipv6_subnet_router_egressors.remove(0))
                .processor(processors::Ipv6Encap)
                .build_link();

            let (_, mut ipv6_encap_interface1_egressors) = ProcessLink::new()
                .ingressor(ipv6_subnet_router_egressors.remove(0))
                .processor(processors::Ipv6Encap)
                .build_link();

            let (_, mut ipv6_encap_interface2_egressors) = ProcessLink::new()
                .ingressor(ipv6_subnet_router_egressors.remove(0))
                .processor(processors::Ipv6Encap)
                .build_link();

            //---------Join to interfaces--------------//
            let mut interfaces = vec![];

            let (mut join0_runnables, mut interface0) = JoinLink::new()
                .ingressor(ipv4_encap_interface0_egressors.remove(0))
                .ingressor(ipv6_encap_interface0_egressors.remove(0))
                .build_link();
            all_runnables.append(&mut join0_runnables);
            interfaces.append(&mut interface0);

            let (mut join1_runnables, mut interface1) = JoinLink::new()
                .ingressor(ipv4_encap_interface1_egressors.remove(0))
                .ingressor(ipv6_encap_interface1_egressors.remove(0))
                .build_link();
            all_runnables.append(&mut join1_runnables);
            interfaces.append(&mut interface1);

            let (mut join2_runnables, mut interface2) = JoinLink::new()
                .ingressor(ipv4_encap_interface2_egressors.remove(0))
                .ingressor(ipv6_encap_interface2_egressors.remove(0))
                .build_link();
            all_runnables.append(&mut join2_runnables);
            interfaces.append(&mut interface2);

            //---------Return built Link!--------------//
            (all_runnables, interfaces)
        }
    }
}
