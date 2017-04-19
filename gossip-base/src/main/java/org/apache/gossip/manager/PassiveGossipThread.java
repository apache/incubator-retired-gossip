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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.gossip.model.Base;
import org.apache.log4j.Logger;


/**
 * This class handles the passive cycle,
 * where this client has received an incoming message. 
 */
public class PassiveGossipThread implements Runnable {

  public static final Logger LOGGER = Logger.getLogger(PassiveGossipThread.class);

  
  private final AtomicBoolean keepRunning;
  private final GossipCore gossipCore;
  private final GossipManager gossipManager;

  public PassiveGossipThread(GossipManager gossipManager, GossipCore gossipCore) {
    this.gossipManager = gossipManager;
    this.gossipCore = gossipCore;
    if (gossipManager.getMyself().getClusterName() == null){
      throw new IllegalArgumentException("Cluster was null");
    }
    
    keepRunning = new AtomicBoolean(true);
  }

  @Override
  public void run() {
    while (keepRunning.get()) {
      try {
        byte[] buf = gossipManager.getTransportManager().read();
        try {
          Base message = gossipManager.getProtocolManager().read(buf);
          gossipCore.receive(message);
          gossipManager.getMemberStateRefresher().run();
        } catch (RuntimeException ex) {//TODO trap json exception
          LOGGER.error("Unable to process message", ex);
        }
      } catch (IOException e) {
        // InterruptedException are completely normal here because of the blocking lifecycle.
        if (!(e.getCause() instanceof InterruptedException)) {
          LOGGER.error(e);
        }
        keepRunning.set(false);
      }
    }
  }
  
  public void requestStop() {
    keepRunning.set(false);
  }
}