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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.gossip.model.Base;
import org.apache.gossip.model.SignedPayload;
import org.apache.log4j.Logger;

import com.codahale.metrics.Meter;

/**
 * This class handles the passive cycle,
 * where this client has received an incoming message. 
 */
abstract public class PassiveGossipThread implements Runnable {

  public static final Logger LOGGER = Logger.getLogger(PassiveGossipThread.class);

  /** The socket used for the passive thread of the gossip service. */
  private final DatagramSocket server;
  private final AtomicBoolean keepRunning;
  private final GossipCore gossipCore;
  private final GossipManager gossipManager;
  private final Meter signed;
  private final Meter unsigned;

  public PassiveGossipThread(GossipManager gossipManager, GossipCore gossipCore) {
    this.gossipManager = gossipManager;
    this.gossipCore = gossipCore;
    if (gossipManager.getMyself().getClusterName() == null){
      throw new IllegalArgumentException("Cluster was null");
    }
    try {
      SocketAddress socketAddress = new InetSocketAddress(gossipManager.getMyself().getUri().getHost(),
              gossipManager.getMyself().getUri().getPort());
      server = new DatagramSocket(socketAddress);
    } catch (SocketException ex) {
      LOGGER.warn(ex);
      throw new RuntimeException(ex);
    }
    keepRunning = new AtomicBoolean(true);
    signed = gossipManager.getRegistry().meter(PassiveGossipConstants.SIGNED_MESSAGE);
    unsigned = gossipManager.getRegistry().meter(PassiveGossipConstants.UNSIGNED_MESSAGE);
  }

  @Override
  public void run() {
    while (keepRunning.get()) {
      try {
        byte[] buf = new byte[server.getReceiveBufferSize()];
        DatagramPacket p = new DatagramPacket(buf, buf.length);
        server.receive(p);
        debug(p.getData());
        try {
          Base activeGossipMessage = gossipManager.getObjectMapper().readValue(p.getData(), Base.class);
          if (activeGossipMessage instanceof SignedPayload){
            SignedPayload s = (SignedPayload) activeGossipMessage;
            Base nested = gossipManager.getObjectMapper().readValue(s.getData(), Base.class);
            gossipCore.receive(nested);
            signed.mark();
          } else {
            gossipCore.receive(activeGossipMessage);
            unsigned.mark();
          }
        } catch (RuntimeException ex) {//TODO trap json exception
          LOGGER.error("Unable to process message", ex);
        }
      } catch (IOException e) {
        LOGGER.error(e);
        keepRunning.set(false);
      }
    }
    shutdown();
  }

  private void debug(byte[] jsonBytes) {
    if (LOGGER.isDebugEnabled()){
      String receivedMessage = new String(jsonBytes);
      LOGGER.debug("Received message ( bytes): " + receivedMessage);
    }
  }

  public void shutdown() {
    try {
      server.close();
    } catch (RuntimeException ex) {
    }
  }

}