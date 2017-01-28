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
import org.apache.gossip.GossipService;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.RemoteGossipMember;
import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class RingPersistenceTest {

  @Test
  public void givenThatRingIsPersisted() throws UnknownHostException, InterruptedException, URISyntaxException {
    GossipSettings settings = new GossipSettings();
    File f = aGossiperPersists(settings);
    Assert.assertTrue(f.exists());
    aNewInstanceGetsRingInfo(settings);
    f.delete();
  }
  
  private File aGossiperPersists(GossipSettings settings) throws UnknownHostException, InterruptedException, URISyntaxException {
    GossipService gossipService = new GossipService("a", new URI("udp://" + "127.0.0.1" + ":" + (29000 + 1)), "1", new HashMap<String, String>(),
            Arrays.asList(
                    new RemoteGossipMember("a", new URI("udp://" + "127.0.0.1" + ":" + (29000 + 0)), "0"),
                    new RemoteGossipMember("a", new URI("udp://" + "127.0.0.1" + ":" + (29000 + 2)), "2")
                    ),
            settings, (a, b) -> { }, new MetricRegistry());
    gossipService.getGossipManager().getRingState().writeToDisk();
    return gossipService.getGossipManager().getRingState().computeTarget();
  }
  
  private void aNewInstanceGetsRingInfo(GossipSettings settings) throws UnknownHostException, InterruptedException, URISyntaxException{
    GossipService gossipService2 = new GossipService("a", new URI("udp://" + "127.0.0.1" + ":" + (29000 + 1)), "1", new HashMap<String, String>(),
            Arrays.asList(),
            settings, (a, b) -> { }, new MetricRegistry());
    Assert.assertEquals(2, gossipService2.getGossipManager().getMembers().size());
  }
  
}
