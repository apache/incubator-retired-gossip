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
package org.apache.gossip.manager.impl;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.gossip.GossipService;
import org.apache.gossip.LocalGossipMember;
import org.apache.gossip.manager.ActiveGossipThread;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.ActiveGossipMessage;
import org.apache.gossip.model.GossipMember;
import org.codehaus.jackson.map.ObjectMapper;

abstract public class SendMembersActiveGossipThread extends ActiveGossipThread {

  protected ObjectMapper om = new ObjectMapper();
  
  public SendMembersActiveGossipThread(GossipManager gossipManager) {
    super(gossipManager);
  }

  private GossipMember convert(LocalGossipMember member){
    GossipMember gm = new GossipMember();
    gm.setCluster(member.getClusterName());
    gm.setHeartbeat(member.getHeartbeat());
    gm.setUri(member.getUri().toASCIIString());
    gm.setId(member.getId());
    return gm;
  }
  
  /**
   * Performs the sending of the membership list, after we have incremented our own heartbeat.
   */
  protected void sendMembershipList(LocalGossipMember me, List<LocalGossipMember> memberList) {
    GossipService.LOGGER.debug("Send sendMembershipList() is called.");
    me.setHeartbeat(System.currentTimeMillis());
    LocalGossipMember member = selectPartner(memberList);
    if (member == null) {
      return;
    }
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setSoTimeout(gossipManager.getSettings().getGossipInterval());
      InetAddress dest = InetAddress.getByName(member.getUri().getHost());
      ActiveGossipMessage message = new ActiveGossipMessage();
      message.getMembers().add(convert(me));
      for (LocalGossipMember other : memberList) {
        message.getMembers().add(convert(other));
      }
      byte[] json_bytes = om.writeValueAsString(message).getBytes();
      int packet_length = json_bytes.length;
      if (packet_length < GossipManager.MAX_PACKET_SIZE) {
        byte[] buf = createBuffer(packet_length, json_bytes);
        DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest, member.getUri().getPort());
        socket.send(datagramPacket);
      } else {
        GossipService.LOGGER.error("The length of the to be send message is too large ("
                + packet_length + " > " + GossipManager.MAX_PACKET_SIZE + ").");
      }
    } catch (IOException e1) {
      GossipService.LOGGER.warn(e1);
    }
  }

  private byte[] createBuffer(int packetLength, byte[] jsonBytes) {
    byte[] lengthBytes = new byte[4];
    lengthBytes[0] = (byte) (packetLength >> 24);
    lengthBytes[1] = (byte) ((packetLength << 8) >> 24);
    lengthBytes[2] = (byte) ((packetLength << 16) >> 24);
    lengthBytes[3] = (byte) ((packetLength << 24) >> 24);
    ByteBuffer byteBuffer = ByteBuffer.allocate(4 + jsonBytes.length);
    byteBuffer.put(lengthBytes);
    byteBuffer.put(jsonBytes);
    byte[] buf = byteBuffer.array();
    return buf;
  }

}
