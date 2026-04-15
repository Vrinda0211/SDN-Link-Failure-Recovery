from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER,MAIN_DISPATCHER,set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet,ethernet,ether_types
from collections import deque
import logging

TOPO={
1:{2:(2,1),3:(3,1)},
2:{1:(1,2),4:(2,2)},
3:{1:(1,3),4:(2,3)},
4:{2:(2,1),3:(3,1)},
}

HOST_PORT={
1:1,
4:1,
}

class SDNController(app_manager.RyuApp):
    OFP_VERSIONS=[ofproto_v1_3.OFP_VERSION]

    def __init__(self,*args,**kwargs):
        super(SDNController,self).__init__(*args,**kwargs)
        self.logger.setLevel(logging.INFO)
        self.graph={dpid:dict(nb) for dpid,nb in TOPO.items()}
        self.mac_to_port={}
        self.host_mac_to_switch={}
        self.datapaths={}

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures,CONFIG_DISPATCHER)
    def switch_features_handler(self,ev):
        dp=ev.msg.datapath
        ofp=dp.ofproto
        parser=dp.ofproto_parser
        self.datapaths[dp.id]=dp
        self.mac_to_port.setdefault(dp.id,{})
        match=parser.OFPMatch()
        actions=[parser.OFPActionOutput(ofp.OFPP_CONTROLLER,ofp.OFPCML_NO_BUFFER)]
        self._add_flow(dp,priority=0,match=match,actions=actions)
        self.logger.info("s%d connected — table-miss installed",dp.id)

    @set_ev_cls(ofp_event.EventOFPPortStatus,MAIN_DISPATCHER)
    def port_status_handler(self,ev):
        msg=ev.msg
        dp=msg.datapath
        ofp=dp.ofproto
        reason=msg.reason
        port_no=msg.desc.port_no
        state=msg.desc.state
        self.logger.info("PortStatus s%d port=%d reason=%d state=0x%x",dp.id,port_no,reason,state)
        link_down=(reason==ofp.OFPPR_DELETE or(reason==ofp.OFPPR_MODIFY and bool(state&ofp.OFPPS_LINK_DOWN)))
        link_up=(reason==ofp.OFPPR_ADD or(reason==ofp.OFPPR_MODIFY and not bool(state&ofp.OFPPS_LINK_DOWN)))
        if link_down:
            self.logger.warning("*** LINK DOWN s%d port %d — rerouting",dp.id,port_no)
            self._handle_link_failure(dp.id,port_no)
        elif link_up:
            self.logger.info("*** LINK UP s%d port %d — restoring",dp.id,port_no)
            self._handle_link_recovery(dp.id,port_no)

    def _handle_link_failure(self,dpid,port_no):
        neighbor=self._neighbor_on_port(dpid,port_no)
        if neighbor is None:
            self.logger.info("Port %d on s%d not an inter-switch link — ignoring",port_no,dpid)
            return
        self.logger.warning("Removing s%d <-> s%d from graph",dpid,neighbor)
        self.graph[dpid].pop(neighbor,None)
        self.graph[neighbor].pop(dpid,None)
        self._flush_all_flows()
        self.host_mac_to_switch.clear()
        self.mac_to_port={d:{} for d in self.mac_to_port}
        self.logger.info("Graph after failure: %s",{k:list(v.keys()) for k,v in self.graph.items()})

    def _handle_link_recovery(self,dpid,port_no):
        for neighbor,ports in TOPO.get(dpid,{}).items():
            if ports[0]==port_no:
                self.graph[dpid][neighbor]=ports
                self.graph[neighbor][dpid]=(ports[1],ports[0])
                self.logger.info("Restored s%d <-> s%d",dpid,neighbor)
                break

    def _neighbor_on_port(self,dpid,port_no):
        for neighbor,(out_port,_) in TOPO.get(dpid,{}).items():
            if out_port==port_no:
                return neighbor
        return None

    @set_ev_cls(ofp_event.EventOFPPacketIn,MAIN_DISPATCHER)
    def packet_in_handler(self,ev):
        msg=ev.msg
        dp=msg.datapath
        ofp=dp.ofproto
        parser=dp.ofproto_parser
        in_port=msg.match['in_port']
        pkt=packet.Packet(msg.data)
        eth=pkt.get_protocol(ethernet.ethernet)
        if eth is None:
            return
        if eth.ethertype==ether_types.ETH_TYPE_LLDP:
            return
        src_mac=eth.src
        dst_mac=eth.dst
        dpid=dp.id
        self.mac_to_port.setdefault(dpid,{})
        self.mac_to_port[dpid][src_mac]=in_port
        if in_port==HOST_PORT.get(dpid):
            self.host_mac_to_switch[src_mac]=(dpid,in_port)
            self.logger.info("Learned host %s at s%d port %d",src_mac,dpid,in_port)
        self.logger.debug("PacketIn s%d port=%d  %s → %s",dpid,in_port,src_mac,dst_mac)
        if dst_mac in self.host_mac_to_switch:
            dst_dpid,dst_host_port=self.host_mac_to_switch[dst_mac]
            self._route_and_install(dp,msg,src_mac,dst_mac,dpid,dst_dpid,dst_host_port,in_port)
        else:
            self.logger.debug("dst %s unknown — flooding",dst_mac)
            self._packet_out(dp,msg,[parser.OFPActionOutput(ofp.OFPP_FLOOD)])

    def _bfs_path(self,src,dst):
        if src==dst:
            return [src]
        queue=deque([[src]])
        visited={src}
        while queue:
            path=queue.popleft()
            node=path[-1]
            for nb in self.graph.get(node,{}):
                if nb==dst:
                    return path+[dst]
                if nb not in visited:
                    visited.add(nb)
                    queue.append(path+[nb])
        return None

    def _route_and_install(self,dp,msg,src_mac,dst_mac,src_dpid,dst_dpid,dst_host_port,in_port):
        parser=dp.ofproto_parser
        path=self._bfs_path(src_dpid,dst_dpid)
        if path is None:
            self.logger.warning("No path s%d→s%d — dropping",src_dpid,dst_dpid)
            return
        self.logger.info("Route %s→%s  path: %s",src_mac,dst_mac," → ".join(f"s{d}" for d in path))
        for i,hop in enumerate(path):
            hop_dp=self.datapaths.get(hop)
            if hop_dp is None:
                continue
            out_port=(dst_host_port if i==len(path)-1 else self.graph[hop][path[i+1]][0])
            hop_in=in_port if i==0 else self.graph[hop][path[i-1]][0]
            self._add_flow(hop_dp,priority=10,match=parser.OFPMatch(in_port=hop_in,eth_src=src_mac,eth_dst=dst_mac),actions=[parser.OFPActionOutput(out_port)],idle_timeout=60)
        rev=list(reversed(path))
        src_host_port=HOST_PORT.get(src_dpid,in_port)
        for i,hop in enumerate(rev):
            hop_dp=self.datapaths.get(hop)
            if hop_dp is None:
                continue
            out_port=(src_host_port if i==len(rev)-1 else self.graph[hop][rev[i+1]][0])
            hop_in=(dst_host_port if i==0 else self.graph[hop][rev[i-1]][0])
            self._add_flow(hop_dp,priority=10,match=parser.OFPMatch(in_port=hop_in,eth_src=dst_mac,eth_dst=src_mac),actions=[parser.OFPActionOutput(out_port)],idle_timeout=60)
        first_out=(self.graph[src_dpid][path[1]][0] if len(path)>1 else dst_host_port)
        self._packet_out(dp,msg,[parser.OFPActionOutput(first_out)])

    def _flush_all_flows(self):
        for dpid,dp in self.datapaths.items():
            ofp=dp.ofproto
            parser=dp.ofproto_parser
            match=parser.OFPMatch()
            dp.send_msg(parser.OFPFlowMod(datapath=dp,command=ofp.OFPFC_DELETE,out_port=ofp.OFPP_ANY,out_group=ofp.OFPG_ANY,match=match))
            self._add_flow(dp,priority=0,match=match,actions=[parser.OFPActionOutput(ofp.OFPP_CONTROLLER,ofp.OFPCML_NO_BUFFER)])
            self.logger.info("Flushed flows on s%d — table-miss reinstalled",dpid)

    def _add_flow(self,dp,priority,match,actions,idle_timeout=0,hard_timeout=0):
        ofp=dp.ofproto
        parser=dp.ofproto_parser
        inst=[parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS,actions)]
        dp.send_msg(parser.OFPFlowMod(datapath=dp,priority=priority,match=match,instructions=inst,idle_timeout=idle_timeout,hard_timeout=hard_timeout))

    def _packet_out(self,dp,msg,actions):
        ofp=dp.ofproto
        parser=dp.ofproto_parser
        data=msg.data if msg.buffer_id==ofp.OFP_NO_BUFFER else None
        dp.send_msg(parser.OFPPacketOut(datapath=dp,buffer_id=msg.buffer_id,in_port=msg.match['in_port'],actions=actions,data=data))