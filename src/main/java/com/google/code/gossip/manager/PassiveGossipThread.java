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
package com.google.code.gossip.manager;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.model.ActiveGossipMessage;

/**
 * [The passive thread: reply to incoming gossip request.] This class handles the passive cycle,
 * where this client has received an incoming message. For now, this message is always the
 * membership list, but if you choose to gossip additional information, you will need some logic to
 * determine the incoming message.
 */
abstract public class PassiveGossipThread implements Runnable {

  public static final Logger LOGGER = Logger.getLogger(PassiveGossipThread.class);

  /** The socket used for the passive thread of the gossip service. */
  private final DatagramSocket server;

  private final GossipManager gossipManager;

  private final AtomicBoolean keepRunning;

  private final String cluster;
  
  private final ObjectMapper MAPPER = new ObjectMapper();

  public PassiveGossipThread(GossipManager gossipManager) {
    this.gossipManager = gossipManager;
    try {
      SocketAddress socketAddress = new InetSocketAddress(gossipManager.getMyself().getHost(),
              gossipManager.getMyself().getPort());
      server = new DatagramSocket(socketAddress);
      GossipService.LOGGER.debug("Gossip service successfully initialized on port "
              + gossipManager.getMyself().getPort());
      GossipService.LOGGER.debug("I am " + gossipManager.getMyself());
      cluster = gossipManager.getMyself().getClusterName();
      if (cluster == null){
        throw new IllegalArgumentException("cluster was null");
      }
    } catch (SocketException ex) {
      GossipService.LOGGER.warn(ex);
      throw new RuntimeException(ex);
    }
    keepRunning = new AtomicBoolean(true);
  }

  @Override
  public void run() {
    while (keepRunning.get()) {
      try {
        byte[] buf = new byte[server.getReceiveBufferSize()];
        DatagramPacket p = new DatagramPacket(buf, buf.length);
        server.receive(p);
        int packet_length = 0;
        for (int i = 0; i < 4; i++) {
          int shift = (4 - 1 - i) * 8;
          packet_length += (buf[i] & 0x000000FF) << shift;
        }
        if (packet_length <= GossipManager.MAX_PACKET_SIZE) {
          byte[] json_bytes = new byte[packet_length];
          for (int i = 0; i < packet_length; i++) {
            json_bytes[i] = buf[i + 4];
          }
          if (GossipService.LOGGER.isDebugEnabled()){
            String receivedMessage = new String(json_bytes);
            GossipService.LOGGER.debug("Received message (" + packet_length + " bytes): "
                  + receivedMessage);
          }
          try {
            List<GossipMember> remoteGossipMembers = new ArrayList<>();
            RemoteGossipMember senderMember = null;
            ActiveGossipMessage activeGossipMessage = MAPPER.readValue(json_bytes,
                    ActiveGossipMessage.class);
            for (int i = 0; i < activeGossipMessage.getMembers().size(); i++) {
              RemoteGossipMember member = new RemoteGossipMember(
                      activeGossipMessage.getMembers().get(i).getCluster(),
                      activeGossipMessage.getMembers().get(i).getHost(),
                      activeGossipMessage.getMembers().get(i).getPort(),
                      activeGossipMessage.getMembers().get(i).getId(),
                      activeGossipMessage.getMembers().get(i).getHeartbeat());
              if (!(member.getClusterName().equals(cluster))){
                GossipService.LOGGER.warn("Note a member of this cluster " + i);
                continue;
              }
              // This is the first member found, so this should be the member who is communicating
              // with me.
              if (i == 0) {
                senderMember = member;
              } 
              remoteGossipMembers.add(member);
            }
            mergeLists(gossipManager, senderMember, remoteGossipMembers);
          } catch (RuntimeException ex) {
            GossipService.LOGGER.error("Unable to process message", ex);
          }
        } else {
          GossipService.LOGGER
                  .error("The received message is not of the expected size, it has been dropped.");
        }

      } catch (IOException e) {
        GossipService.LOGGER.error(e);
        System.out.println(e);
        keepRunning.set(false);
      }
    }
    shutdown();
  }

  public void shutdown() {
    try {
      server.close();
    } catch (RuntimeException ex) {
    }
  }

  /**
   * Abstract method for merging the local and remote list.
   * 
   * @param gossipManager
   *          The GossipManager for retrieving the local members and dead members list.
   * @param senderMember
   *          The member who is sending this list, this could be used to send a response if the
   *          remote list contains out-dated information.
   * @param remoteList
   *          The list of members known at the remote side.
   */
  abstract protected void mergeLists(GossipManager gossipManager, RemoteGossipMember senderMember,
          List<GossipMember> remoteList);
}

/*
 * random comments // Check whether the package is smaller than the maximal packet length. // A
 * package larger than this would not be possible to be send from a GossipService, // since this is
 * check before sending the message. // This could normally only occur when the list of members is
 * very big, // or when the packet is malformed, and the first 4 bytes is not the right in anymore.
 * // For this reason we regards the message.
 */
