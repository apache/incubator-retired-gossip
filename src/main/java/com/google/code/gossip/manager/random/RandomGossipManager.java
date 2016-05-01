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
package com.google.code.gossip.manager.random;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.impl.OnlyProcessReceivedPassiveGossipThread;

import java.util.List;

public class RandomGossipManager extends GossipManager {
  public RandomGossipManager(String cluster, String address, int port, String id,
          GossipSettings settings, List<GossipMember> gossipMembers, GossipListener listener) {
    super(OnlyProcessReceivedPassiveGossipThread.class, RandomActiveGossipThread.class, cluster,
            address, port, id, settings, gossipMembers, listener);
  }
}
