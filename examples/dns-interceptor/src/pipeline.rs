// Generated by route-rs-graphgen
// Source graph: examples/dns-interceptor/src/pipeline.xml

use crate::packets::*;
use crate::processors::*;
use route_rs_runtime::link::primitive::*;
use route_rs_runtime::link::*;
use tokio::runtime;

pub struct Pipeline {}

impl route_rs_runtime::pipeline::Runner for Pipeline {
    type Input = (Interface, SimplePacket);
    type Output = (Interface, SimplePacket);

    fn run(
        input_channel: crossbeam::Receiver<Self::Input>,
        output_channel: crossbeam::Sender<Self::Output>,
    ) {
        let mut all_runnables: Vec<TokioRunnable> = vec![];

        let elem_1_setinterfacebydestination = SetInterfaceByDestination::new();
        let elem_2_classifydns = ClassifyDNS::new();
        let elem_3_localdnsinterceptor = LocalDNSInterceptor::new();

        let (mut runnables_1, mut egressors_1) =
            InputChannelLink::new().channel(input_channel).build_link();
        all_runnables.append(&mut runnables_1);
        let link_1_egress_0 = egressors_1.remove(0);

        let (mut runnables_2, mut egressors_2) = ProcessLink::new()
            .ingressor(link_1_egress_0)
            .processor(elem_1_setinterfacebydestination)
            .build_link();
        all_runnables.append(&mut runnables_2);
        let link_2_egress_0 = egressors_2.remove(0);

        let (mut runnables_3, mut egressors_3) = ClassifyLink::new()
            .ingressor(link_2_egress_0)
            .classifier(elem_2_classifydns)
            .dispatcher(Box::new(|c| match c {
                ClassifyDNSOutput::DNS => 0,
                _ => 1,
            }))
            .num_egressors(2)
            .build_link();
        all_runnables.append(&mut runnables_3);
        let link_3_egress_0 = egressors_3.remove(0);
        let link_3_egress_1 = egressors_3.remove(0);

        let (mut runnables_4, mut egressors_4) = ProcessLink::new()
            .ingressor(link_3_egress_0)
            .processor(elem_3_localdnsinterceptor)
            .build_link();
        all_runnables.append(&mut runnables_4);
        let link_4_egress_0 = egressors_4.remove(0);

        let (mut runnables_5, mut egressors_5) = JoinLink::new()
            .ingressors(vec![link_4_egress_0, link_3_egress_1])
            .build_link();
        all_runnables.append(&mut runnables_5);
        let link_5_egress_0 = egressors_5.remove(0);

        let (mut runnables_6, mut _egressors_6) = OutputChannelLink::new()
            .ingressor(link_5_egress_0)
            .channel(output_channel)
            .build_link();
        all_runnables.append(&mut runnables_6);

        let mut rt = runtime::Builder::new().enable_all().build().unwrap();

        rt.block_on(async move {
            let mut handles = vec![];
            for runnable in all_runnables {
                handles.push(tokio::spawn(runnable));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}
