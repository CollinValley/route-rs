use route_rs_packets::{EthernetFrame, MacAddr};
use std::net::IpAddr;

pub(crate) enum ArpOp {
    Request = 1,
    Reply = 2,
}

pub(crate) enum ArpHardwareType {
    Ethernet = 1,
}

pub(crate) const ARP_ETHER_TYPE: u16 = 0x0806;

// NOTE: Could be implemented in various ways, such as a specialized version of EthernetFrame that's
// known to be an ARP frame. It could be implemented in a similar way that packets are promoted/demoted
// with `TryFrom`.
#[derive(Clone)]
pub(crate) struct ArpFrame {
    frame: EthernetFrame,
}

// TODO: remove after finished ARP implementation
#[allow(dead_code)]
impl ArpFrame {
    pub fn new(frame: EthernetFrame) -> Self {
        assert_eq!(frame.ether_type(), ARP_ETHER_TYPE);
        ArpFrame { frame }
    }

    pub fn hardware_type(&self) -> u16 {
        unimplemented!()
    }

    pub fn protocol_type(&self) -> u16 {
        unimplemented!()
    }

    pub fn hardware_addr_len(&self) -> u8 {
        unimplemented!()
    }

    pub fn protocol_addr_len(&self) -> u8 {
        unimplemented!()
    }

    pub fn opcode(&self) -> u8 {
        unimplemented!()
    }

    pub fn sender_hardware_addr(&self) -> &[u8] {
        unimplemented!()
    }

    pub fn sender_protocol_addr(&self) -> &[u8] {
        unimplemented!()
    }

    pub fn target_hardware_addr(&self) -> &[u8] {
        unimplemented!()
    }

    pub fn target_protocol_addr(&self) -> &[u8] {
        unimplemented!()
    }

    pub fn set_hardware_type(&self, _htype: u16) {
        unimplemented!()
    }

    pub fn set_protocol_type(&self, _ptype: u16) {
        unimplemented!()
    }

    pub fn set_hardware_addr_len(&self, _len: u8) {
        unimplemented!()
    }

    pub fn set_protocol_addr_len(&self, _len: u8) {
        unimplemented!()
    }

    pub fn set_opcode(&mut self, _code: u8) {
        unimplemented!()
    }

    pub fn set_sender_hardware_addr(&mut self, _addr: MacAddr) {
        unimplemented!()
    }

    pub fn set_sender_protocol_addr(&mut self, _ip_addr: IpAddr) {
        unimplemented!()
    }

    pub fn set_target_hardware_addr(&mut self, _addr: MacAddr) {
        unimplemented!()
    }

    pub fn set_target_protocol_addr(&mut self, _ip_addr: IpAddr) {
        unimplemented!()
    }

    pub fn frame(self) -> EthernetFrame {
        self.frame
    }
}
