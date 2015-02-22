package com.google.code.gossip.manager.impl;

import java.util.List;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.PassiveGossipThread;

public class OnlyProcessReceivedPassiveGossipThread extends PassiveGossipThread {

  public OnlyProcessReceivedPassiveGossipThread(GossipManager gossipManager) {
    super(gossipManager);
  }

  /**
   * Merge remote list (received from peer), and our local member list. Simply, we must update the
   * heartbeats that the remote list has with our list. Also, some additional logic is needed to
   * make sure we have not timed out a member and then immediately received a list with that member.
   *
   * @param gossipManager
   * @param senderMember
   * @param remoteList
   */
  protected void mergeLists(GossipManager gossipManager, RemoteGossipMember senderMember,
          List<GossipMember> remoteList) {

    for (GossipMember remoteMember : remoteList) {
      // Skip myself. We don't want ourselves in the local member list.
      if (remoteMember.equals(gossipManager.getMyself())) {
        continue;
      }
      if (gossipManager.getMemberList().contains(remoteMember)) {
        LocalGossipMember localMember = gossipManager.getMemberList().get(
                gossipManager.getMemberList().indexOf(remoteMember));
        if (remoteMember.getHeartbeat() > localMember.getHeartbeat()) {
          localMember.setHeartbeat(remoteMember.getHeartbeat());
          localMember.resetTimeoutTimer();
        }
      } else {
        // The remote member is either brand new, or a previously declared dead member.
        // If its dead, check the heartbeat because it may have come back from the dead.
        if (gossipManager.getDeadList().contains(remoteMember)) {
          // The remote member is known here as a dead member.
          GossipService.LOGGER.debug("The remote member is known here as a dead member.");
          LocalGossipMember localDeadMember = gossipManager.getDeadList().get(
                  gossipManager.getDeadList().indexOf(remoteMember));
          // If a member is restarted the heartbeat will restart from 1, so we should check
          // that here.
          // So a member can become from the dead when it is either larger than a previous
          // heartbeat (due to network failure)
          // or when the heartbeat is 1 (after a restart of the service).
          // TODO: What if the first message of a gossip service is sent to a dead node? The
          // second member will receive a heartbeat of two.
          // TODO: The above does happen. Maybe a special message for a revived member?
          // TODO: Or maybe when a member is declared dead for more than
          // _settings.getCleanupInterval() ms, reset the heartbeat to 0.
          // It will then accept a revived member.
          // The above is now handle by checking whether the heartbeat differs
          // _settings.getCleanupInterval(), it must be restarted.
          if (remoteMember.getHeartbeat() == 1
                  || ((localDeadMember.getHeartbeat() - remoteMember.getHeartbeat()) * -1) > (gossipManager
                          .getSettings().getCleanupInterval() / 1000)
                  || remoteMember.getHeartbeat() > localDeadMember.getHeartbeat()) {
            GossipService.LOGGER
                    .debug("The remote member is back from the dead. We will remove it from the dead list and add it as a new member.");
            // The remote member is back from the dead.
            // Remove it from the dead list.
            // gossipManager.getDeadList().remove(localDeadMember);
            // Add it as a new member and add it to the member list.
            LocalGossipMember newLocalMember = new LocalGossipMember(remoteMember.getHost(),
                    remoteMember.getPort(), remoteMember.getId(), remoteMember.getHeartbeat(),
                    gossipManager, gossipManager.getSettings().getCleanupInterval());
            // gossipManager.getMemberList().add(newLocalMember);
            gossipManager.createOrRevivieMember(newLocalMember);
            newLocalMember.startTimeoutTimer();
            GossipService.LOGGER.info("Removed remote member " + remoteMember.getAddress()
                    + " from dead list and added to local member list.");
          }
        } else {
          // Brand spanking new member - welcome.
          LocalGossipMember newLocalMember = new LocalGossipMember(remoteMember.getHost(),
                  remoteMember.getPort(), remoteMember.getId(), remoteMember.getHeartbeat(),
                  gossipManager, gossipManager.getSettings().getCleanupInterval());
          gossipManager.createOrRevivieMember(newLocalMember);
          newLocalMember.startTimeoutTimer();
          GossipService.LOGGER.info("Added new remote member " + remoteMember.getAddress()
                  + " to local member list.");
        }
      }
    }
  }

}
