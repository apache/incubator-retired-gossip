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
package com.google.code.gossip.manager.impl;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.json.JSONArray;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.manager.ActiveGossipThread;
import com.google.code.gossip.manager.GossipManager;

abstract public class SendMembersActiveGossipThread extends ActiveGossipThread {

  public SendMembersActiveGossipThread(GossipManager gossipManager) {
    super(gossipManager);
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
    try (DatagramSocket socket = new DatagramSocket()){
      socket.setSoTimeout(_gossipManager.getSettings().getGossipInterval());
      InetAddress dest = InetAddress.getByName(member.getHost());
      JSONArray jsonArray = new JSONArray();
      jsonArray.put(me.toJSONObject());
      for (LocalGossipMember other : memberList) {
        jsonArray.put(other.toJSONObject());
        GossipService.LOGGER.debug(other);
      }
      byte[] json_bytes = jsonArray.toString().getBytes();
      int packet_length = json_bytes.length;
      if (packet_length < GossipManager.MAX_PACKET_SIZE) {
        byte[] buf = createBuffer(packet_length, json_bytes);
        DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest,
                member.getPort());
        socket.send(datagramPacket);
      } else {
        GossipService.LOGGER.error("The length of the to be send message is too large ("
                + packet_length + " > " + GossipManager.MAX_PACKET_SIZE + ").");
      }
    } catch (IOException e1) {
      GossipService.LOGGER.warn(e1);
    }
  }
  
  private byte[] createBuffer(int packetLength, byte [] jsonBytes){
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
