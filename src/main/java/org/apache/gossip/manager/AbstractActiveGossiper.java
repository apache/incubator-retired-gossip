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
package org.apache.gossip.manager;

import java.util.Map.Entry;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.apache.gossip.LocalGossipMember;
import org.apache.gossip.model.ActiveGossipOk;
import org.apache.gossip.model.GossipDataMessage;
import org.apache.gossip.model.GossipMember;
import org.apache.gossip.model.Response;
import org.apache.gossip.model.SharedGossipDataMessage;
import org.apache.gossip.model.ShutdownMessage;
import org.apache.gossip.udp.UdpActiveGossipMessage;
import org.apache.gossip.udp.UdpGossipDataMessage;
import org.apache.gossip.udp.UdpSharedGossipDataMessage;
import org.apache.log4j.Logger;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * The ActiveGossipThread is sends information. Pick a random partner and send the membership list to that partner
 */
public abstract class AbstractActiveGossiper {

  protected static final Logger LOGGER = Logger.getLogger(AbstractActiveGossiper.class);
  
  protected final GossipManager gossipManager;
  protected final GossipCore gossipCore;
  private final Histogram sharedDataHistogram;
  private final Histogram sendPerNodeDataHistogram;
  private final Histogram sendMembershipHistorgram;
  private final Random random;

  public AbstractActiveGossiper(GossipManager gossipManager, GossipCore gossipCore, MetricRegistry registry) {
    this.gossipManager = gossipManager;
    this.gossipCore = gossipCore;
    sharedDataHistogram = registry.histogram(name(AbstractActiveGossiper.class, "sharedDataHistogram-time"));
    sendPerNodeDataHistogram = registry.histogram(name(AbstractActiveGossiper.class, "sendPerNodeDataHistogram-time"));
    sendMembershipHistorgram = registry.histogram(name(AbstractActiveGossiper.class, "sendMembershipHistorgram-time"));
    random = new Random();
  }

  public void init() {

  }
  
  public void shutdown() {

  }

  public final void sendShutdownMessage(LocalGossipMember me, LocalGossipMember target){
    if (target == null){
      return;
    }
    ShutdownMessage m = new ShutdownMessage();
    m.setNodeId(me.getId());
    m.setShutdownAtNanos(gossipManager.getClock().nanoTime());
    gossipCore.sendOneWay(m, target.getUri());
  }
  
  public final void sendSharedData(LocalGossipMember me, LocalGossipMember member){
    if (member == null){
      return;
    }
    long startTime = System.currentTimeMillis();
    for (Entry<String, SharedGossipDataMessage> innerEntry : gossipCore.getSharedData().entrySet()){
      UdpSharedGossipDataMessage message = new UdpSharedGossipDataMessage();
      message.setUuid(UUID.randomUUID().toString());
      message.setUriFrom(me.getId());
      message.setExpireAt(innerEntry.getValue().getExpireAt());
      message.setKey(innerEntry.getValue().getKey());
      message.setNodeId(innerEntry.getValue().getNodeId());
      message.setTimestamp(innerEntry.getValue().getTimestamp());
      message.setPayload(innerEntry.getValue().getPayload());
      gossipCore.sendOneWay(message, member.getUri());
    }
    sharedDataHistogram.update(System.currentTimeMillis() - startTime);
  }
  
  public final void sendPerNodeData(LocalGossipMember me, LocalGossipMember member){
    if (member == null){
      return;
    }
    long startTime = System.currentTimeMillis();
    for (Entry<String, ConcurrentHashMap<String, GossipDataMessage>> entry : gossipCore.getPerNodeData().entrySet()){
      for (Entry<String, GossipDataMessage> innerEntry : entry.getValue().entrySet()){
        UdpGossipDataMessage message = new UdpGossipDataMessage();
        message.setUuid(UUID.randomUUID().toString());
        message.setUriFrom(me.getId());
        message.setExpireAt(innerEntry.getValue().getExpireAt());
        message.setKey(innerEntry.getValue().getKey());
        message.setNodeId(innerEntry.getValue().getNodeId());
        message.setTimestamp(innerEntry.getValue().getTimestamp());
        message.setPayload(innerEntry.getValue().getPayload());
        gossipCore.sendOneWay(message, member.getUri());   
      }
    }
    sendPerNodeDataHistogram.update(System.currentTimeMillis() - startTime);
  }
    
  /**
   * Performs the sending of the membership list, after we have incremented our own heartbeat.
   */
  protected void sendMembershipList(LocalGossipMember me, LocalGossipMember member) {
    if (member == null){
      return;
    }
    long startTime = System.currentTimeMillis();
    me.setHeartbeat(System.nanoTime());
    UdpActiveGossipMessage message = new UdpActiveGossipMessage();
    message.setUriFrom(gossipManager.getMyself().getUri().toASCIIString());
    message.setUuid(UUID.randomUUID().toString());
    message.getMembers().add(convert(me));
    for (LocalGossipMember other : gossipManager.getMembers().keySet()) {
      message.getMembers().add(convert(other));
    }
    Response r = gossipCore.send(message, member.getUri());
    if (r instanceof ActiveGossipOk){
      //maybe count metrics here
    } else {
      LOGGER.debug("Message " + message + " generated response " + r);
    }
    sendMembershipHistorgram.update(System.currentTimeMillis() - startTime);
  }
    
  protected final GossipMember convert(LocalGossipMember member){
    GossipMember gm = new GossipMember();
    gm.setCluster(member.getClusterName());
    gm.setHeartbeat(member.getHeartbeat());
    gm.setUri(member.getUri().toASCIIString());
    gm.setId(member.getId());
    gm.setProperties(member.getProperties());
    return gm;
  }
  
  /**
   * 
   * @param memberList
   *          An immutable list
   * @return The chosen LocalGossipMember to gossip with.
   */
  protected LocalGossipMember selectPartner(List<LocalGossipMember> memberList) {
    LocalGossipMember member = null;
    if (memberList.size() > 0) {
      int randomNeighborIndex = random.nextInt(memberList.size());
      member = memberList.get(randomNeighborIndex);
    }
    return member;
  }
}
