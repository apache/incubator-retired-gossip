package org.apache.gossip.model;

import org.apache.gossip.udp.UdpActiveGossipMessage;
import org.apache.gossip.udp.UdpActiveGossipOk;
import org.apache.gossip.udp.UdpGossipDataMessage;
import org.apache.gossip.udp.UdpNotAMemberFault;
import org.apache.gossip.udp.UdpSharedGossipDataMessage;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(  
        use = JsonTypeInfo.Id.CLASS,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type") 
@JsonSubTypes({
        @Type(value = ActiveGossipMessage.class, name = "ActiveGossipMessage"),
        @Type(value = Fault.class, name = "Fault"),
        @Type(value = ActiveGossipOk.class, name = "ActiveGossipOk"),
        @Type(value = UdpActiveGossipOk.class, name = "UdpActiveGossipOk"),
        @Type(value = UdpActiveGossipMessage.class, name = "UdpActiveGossipMessage"),
        @Type(value = UdpNotAMemberFault.class, name = "UdpNotAMemberFault"),
        @Type(value = GossipDataMessage.class, name = "GossipDataMessage"),
        @Type(value = UdpGossipDataMessage.class, name = "UdpGossipDataMessage"),
        @Type(value = SharedGossipDataMessage.class, name = "SharedGossipDataMessage"),
        @Type(value = UdpSharedGossipDataMessage.class, name = "UdpSharedGossipDataMessage")
        })
public class Base {

}
