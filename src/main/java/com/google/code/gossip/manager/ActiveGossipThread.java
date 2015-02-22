package com.google.code.gossip.manager;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;

/**
 * [The active thread: periodically send gossip request.] The class handles gossiping the membership
 * list. This information is important to maintaining a common state among all the nodes, and is
 * important for detecting failures.
 */
abstract public class ActiveGossipThread implements Runnable {

  private final GossipManager _gossipManager;

  private final AtomicBoolean _keepRunning;

  public ActiveGossipThread(GossipManager gossipManager) {
    _gossipManager = gossipManager;
    _keepRunning = new AtomicBoolean(true);
  }

  @Override
  public void run() {
    while (_keepRunning.get()) {
      try {
        TimeUnit.MILLISECONDS.sleep(_gossipManager.getSettings().getGossipInterval());
        sendMembershipList(_gossipManager.getMyself(), _gossipManager.getMemberList());
      } catch (InterruptedException e) {
        GossipService.LOGGER.error(e);
        _keepRunning.set(false);
      }
    }
    shutdown();
  }

  public void shutdown() {
    _keepRunning.set(false);
  }

  /**
   * Performs the sending of the membership list, after we have incremented our own heartbeat.
   */
  abstract protected void sendMembershipList(LocalGossipMember me,
          List<LocalGossipMember> memberList);

  /**
   * Abstract method which should be implemented by a subclass. This method should return a member
   * of the list to gossip with.
   *
   * @param memberList
   *          The list of members which are stored in the local list of members.
   * @return The chosen LocalGossipMember to gossip with.
   */
  abstract protected LocalGossipMember selectPartner(List<LocalGossipMember> memberList);
}
