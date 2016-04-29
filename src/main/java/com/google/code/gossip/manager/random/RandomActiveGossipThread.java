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

import java.util.List;
import java.util.Random;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.impl.SendMembersActiveGossipThread;

public class RandomActiveGossipThread extends SendMembersActiveGossipThread {

  /** The Random used for choosing a member to gossip with. */
  private final Random random;

  public RandomActiveGossipThread(GossipManager gossipManager) {
    super(gossipManager);
    random = new Random();
  }

  /**
   * [The selectToSend() function.] Find a random peer from the local membership list. In the case
   * where this client is the only member in the list, this method will return null.
   * 
   * @return Member random member if list is greater than 1, null otherwise
   */
  protected LocalGossipMember selectPartner(List<LocalGossipMember> memberList) {
    LocalGossipMember member = null;
    if (memberList.size() > 0) {
      int randomNeighborIndex = random.nextInt(memberList.size());
      member = memberList.get(randomNeighborIndex);
    } else {
      GossipService.LOGGER.debug("I am alone in this world.");
    }
    return member;
  }

}
