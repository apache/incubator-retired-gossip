/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
