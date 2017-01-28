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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.gossip.GossipService;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.model.GossipDataMessage;
import org.apache.gossip.model.SharedGossipDataMessage;
import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class UserDataPersistenceTest {

  @Test
  public void givenThatRingIsPersisted() throws UnknownHostException, InterruptedException, URISyntaxException {
    String nodeId = "1";
    GossipSettings settings = new GossipSettings();
    { //Create a gossip service and force it to persist its user data
      GossipService gossipService = new GossipService("a",
              new URI("udp://" + "127.0.0.1" + ":" + (29000 + 1)), nodeId, new HashMap<String, String>(),
              Arrays.asList(), settings, (a, b) -> { }, new MetricRegistry());
      gossipService.start();
      gossipService.gossipPerNodeData(getToothpick());
      gossipService.gossipSharedData(getAnotherToothpick());
      gossipService.getGossipManager().getUserDataState().writePerNodeToDisk();
      gossipService.getGossipManager().getUserDataState().writeSharedToDisk();
      { //read the raw data and confirm
        ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>> l = gossipService.getGossipManager().getUserDataState().readPerNodeFromDisk();
        Assert.assertEquals("red", ((AToothpick) l.get(nodeId).get("a").getPayload()).getColor());
      }
      {
        ConcurrentHashMap<String, SharedGossipDataMessage> l = 
                gossipService.getGossipManager().getUserDataState().readSharedDataFromDisk();
        Assert.assertEquals("blue", ((AToothpick) l.get("a").getPayload()).getColor());
      }
      gossipService.shutdown();
    }
    { //recreate the service and see that the data is read back in
      GossipService gossipService = new GossipService("a",
              new URI("udp://" + "127.0.0.1" + ":" + (29000 + 1)), nodeId, new HashMap<String, String>(),
              Arrays.asList(), settings, (a, b) -> { }, new MetricRegistry());
      gossipService.start();
      Assert.assertEquals("red", ((AToothpick) gossipService.findPerNodeData(nodeId, "a").getPayload()).getColor());
      Assert.assertEquals("blue", ((AToothpick) gossipService.findSharedData("a").getPayload()).getColor());
      File f = gossipService.getGossipManager().getUserDataState().computeSharedTarget();
      File g = gossipService.getGossipManager().getUserDataState().computePerNodeTarget();
      gossipService.shutdown();
      f.delete();
      g.delete();
    }
  }
  
  public GossipDataMessage getToothpick(){
    AToothpick a = new AToothpick();
    a.setColor("red");
    GossipDataMessage d = new GossipDataMessage();
    d.setExpireAt(Long.MAX_VALUE);
    d.setKey("a");
    d.setPayload(a);
    d.setTimestamp(System.currentTimeMillis());
    return d;
  }
  
  public SharedGossipDataMessage getAnotherToothpick(){
    AToothpick a = new AToothpick();
    a.setColor("blue");
    SharedGossipDataMessage d = new SharedGossipDataMessage();
    d.setExpireAt(Long.MAX_VALUE);
    d.setKey("a");
    d.setPayload(a);
    d.setTimestamp(System.currentTimeMillis());
    return d;
  }
  
  public static class AToothpick {
    private String color;
    public AToothpick(){
      
    }
    public String getColor() {
      return color;
    }
    public void setColor(String color) {
      this.color = color;
    }
    
  }
}
