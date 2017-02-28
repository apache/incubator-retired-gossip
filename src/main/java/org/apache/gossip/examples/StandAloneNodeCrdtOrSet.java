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
package org.apache.gossip.examples;

import com.codahale.metrics.MetricRegistry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.gossip.GossipService;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.RemoteGossipMember;
import org.apache.gossip.crdt.OrSet;
import org.apache.gossip.model.SharedGossipDataMessage;

public class StandAloneNodeCrdtOrSet {
  public static void main (String [] args) throws InterruptedException, IOException{
    GossipSettings s = new GossipSettings();
    s.setWindowSize(10);
    s.setConvictThreshold(1.0);
    s.setGossipInterval(10);
    GossipService gossipService = new GossipService("mycluster",  URI.create(args[0]), args[1], new HashMap<String, String>(),
            Arrays.asList( new RemoteGossipMember("mycluster", URI.create(args[2]), args[3])), s, (a,b) -> {}, new MetricRegistry());
    gossipService.start();
    
    new Thread(() -> {
      while (true){
      System.out.println("Live: " + gossipService.getGossipManager().getLiveMembers());
      System.out.println("Dead: " + gossipService.getGossipManager().getDeadMembers());
      System.out.println("---------- " + (gossipService.getGossipManager().findCrdt("abc") == null ? "": 
          gossipService.getGossipManager().findCrdt("abc").value()));
      System.out.println("********** " + gossipService.getGossipManager().findCrdt("abc"));
      try {
        Thread.sleep(2000);
      } catch (Exception e) {}
      }
    }).start();
    
    String line = null;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))){
      while ( (line = br.readLine()) != null){
        System.out.println(line);
        char op = line.charAt(0);
        String val = line.substring(2);
        if (op == 'a'){
          addData(val, gossipService);
        } else {
          removeData(val, gossipService);
        }
      }
    }
  }
  
  private static void removeData(String val, GossipService gossipService){
    OrSet<String> s = (OrSet<String>) gossipService.getGossipManager().findCrdt("abc");
    SharedGossipDataMessage m = new SharedGossipDataMessage();
    m.setExpireAt(Long.MAX_VALUE);
    m.setKey("abc");
    m.setPayload(new OrSet<String>(s , new OrSet.Builder<String>().remove(val)));
    m.setTimestamp(System.currentTimeMillis());
    gossipService.getGossipManager().merge(m);
  }
  
  private static void addData(String val, GossipService gossipService){
    SharedGossipDataMessage m = new SharedGossipDataMessage();
    m.setExpireAt(Long.MAX_VALUE);
    m.setKey("abc");
    m.setPayload(new OrSet<String>(val));
    m.setTimestamp(System.currentTimeMillis());
    gossipService.getGossipManager().merge(m);
  }
}
