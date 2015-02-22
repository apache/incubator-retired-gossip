package com.google.code.gossip.manager.random;

import java.util.List;
import java.util.Random;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.impl.SendMembersActiveGossipThread;

public class RandomActiveGossipThread extends SendMembersActiveGossipThread {

  /** The Random used for choosing a member to gossip with. */
  private final Random _random;

  public RandomActiveGossipThread(GossipManager gossipManager) {
    super(gossipManager);
    _random = new Random();
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
      int randomNeighborIndex = _random.nextInt(memberList.size());
      member = memberList.get(randomNeighborIndex);
    } else {
      GossipService.LOGGER.debug("I am alone in this world.");
    }
    return member;
  }

}
