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

import com.codahale.metrics.MetricRegistry;
import org.apache.gossip.GossipMember;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalGossipMember;
import org.apache.gossip.manager.handlers.DefaultMessageInvoker;
import org.apache.gossip.manager.handlers.MessageInvoker;
import org.apache.gossip.manager.handlers.ResponseHandler;
import org.apache.gossip.manager.handlers.SimpleMessageInvoker;
import org.apache.gossip.manager.random.RandomGossipManager;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import javax.xml.ws.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.expectThrows;

@RunWith(JUnitPlatform.class)
public class RandomGossipManagerBuilderTest {

  @Test
  public void idShouldNotBeNull() {
    expectThrows(IllegalArgumentException.class,() -> {
        RandomGossipManager.newBuilder().cluster("aCluster").build();
    });
  }

  @Test
  public void clusterShouldNotBeNull() {
      expectThrows(IllegalArgumentException.class,() -> {
          RandomGossipManager.newBuilder().withId("id").build();
      });
  }

  @Test
  public void settingsShouldNotBeNull() {
      expectThrows(IllegalArgumentException.class,() -> {
          RandomGossipManager.newBuilder().withId("id").cluster("aCluster").build();
      });
  }
  
  @Test
  public void createMembersListIfNull() throws URISyntaxException {
    RandomGossipManager gossipManager = RandomGossipManager.newBuilder()
        .withId("id")
        .cluster("aCluster")
        .uri(new URI("udp://localhost:2000"))
        .settings(new GossipSettings())
        .gossipMembers(null).registry(new MetricRegistry()).build();
    assertNotNull(gossipManager.getLiveMembers());
  }

  @Test
  public void createDefaultMessageInvokerIfNull() throws URISyntaxException {
    RandomGossipManager gossipManager = RandomGossipManager.newBuilder()
        .withId("id")
        .cluster("aCluster")
        .uri(new URI("udp://localhost:2000"))
        .settings(new GossipSettings())
        .messageInvoker(null).registry(new MetricRegistry()).build();
    assertNotNull(gossipManager.getMessageInvoker());
    Assert.assertEquals(gossipManager.getMessageInvoker().getClass(), new DefaultMessageInvoker().getClass());
  }

  @Test
  public void testMessageInvokerKeeping() throws URISyntaxException {
    MessageInvoker mi = new SimpleMessageInvoker(Response.class, new ResponseHandler());
    RandomGossipManager gossipManager = RandomGossipManager.newBuilder()
        .withId("id")
        .cluster("aCluster")
        .uri(new URI("udp://localhost:2000"))
        .settings(new GossipSettings())
        .messageInvoker(mi).registry(new MetricRegistry()).build();
    assertNotNull(gossipManager.getMessageInvoker());
    Assert.assertEquals(gossipManager.getMessageInvoker(), mi);
  }

  @Test
  public void useMemberListIfProvided() throws URISyntaxException {
    LocalGossipMember member = new LocalGossipMember(
            "aCluster", new URI("udp://localhost:2000"), "aGossipMember",
            System.nanoTime(), new HashMap<String, String>(), 1000, 1, "exponential");
    List<GossipMember> memberList = new ArrayList<>();
    memberList.add(member);
    RandomGossipManager gossipManager = RandomGossipManager.newBuilder()
        .withId("id")
        .cluster("aCluster")
        .settings(new GossipSettings())
        .uri(new URI("udp://localhost:8000"))
        .gossipMembers(memberList).registry(new MetricRegistry()).build();
    assertEquals(1, gossipManager.getDeadMembers().size());
    assertEquals(member.getId(), gossipManager.getDeadMembers().get(0).getId());
  }

}