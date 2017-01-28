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
package org.apache.gossip;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit; 

import org.apache.gossip.manager.DatacenterRackAwareActiveGossiper;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.codahale.metrics.MetricRegistry;

import io.teknek.tunit.TUnit;

@RunWith(JUnitPlatform.class)
public class IdAndPropertyTest {

  @Test
  public void testDatacenterRackGossiper() throws URISyntaxException, UnknownHostException, InterruptedException{
    GossipSettings settings = new GossipSettings();
    settings.setActiveGossipClass(DatacenterRackAwareActiveGossiper.class.getName());
    List<GossipMember> startupMembers = new ArrayList<>();
    Map<String, String> x = new HashMap<>();
    x.put("a", "b");
    x.put("datacenter", "dc1");
    x.put("rack", "rack1");
    GossipService gossipService1 = new GossipService("a", new URI("udp://" + "127.0.0.1" + ":" + (29000 + 0)), "0", x, startupMembers, settings,
            (a, b) -> {}, new MetricRegistry());
    gossipService1.start();
    
    Map<String, String> y = new HashMap<>();
    y.put("a", "c");
    y.put("datacenter", "dc2");
    y.put("rack", "rack2");
    GossipService gossipService2 = new GossipService("a", new URI("udp://" + "127.0.0.1" + ":" + (29000 + 1)), "1", y,
            Arrays.asList(new RemoteGossipMember("a",
                    new URI("udp://" + "127.0.0.1" + ":" + (29000 + 0)), "0")),
            settings, (a, b) -> { }, new MetricRegistry());
    gossipService2.start();
    TUnit.assertThat(() -> { 
      String value = ""; 
      try {
        value = gossipService1.getGossipManager().getLiveMembers().get(0).getProperties().get("a");
      } catch (RuntimeException e){ }
      return value;
    }).afterWaitingAtMost(10, TimeUnit.SECONDS).isEqualTo("c");
    
    TUnit.assertThat(() -> { 
      String value = ""; 
      try {
        value = gossipService2.getGossipManager().getLiveMembers().get(0).getProperties().get("a");
      } catch (RuntimeException e){ }
      return value;
    }).afterWaitingAtMost(10, TimeUnit.SECONDS).isEqualTo("b");    
    gossipService1.shutdown();
    gossipService2.shutdown();
    
  }
}
