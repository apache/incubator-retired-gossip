package com.google.code.gossip.manager.impl;

import java.util.ArrayList;

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
	 * Merge remote list (received from peer), and our local member list.
	 * Simply, we must update the heartbeats that the remote list has with
	 * our list.  Also, some additional logic is needed to make sure we have 
	 * not timed out a member and then immediately received a list with that 
	 * member.
	 * @param remoteList
	 */
	protected void mergeLists(GossipManager gossipManager, RemoteGossipMember senderMember, ArrayList<GossipMember> remoteList) {

		synchronized (gossipManager.getDeadList()) {

			synchronized (gossipManager.getMemberList()) {

				for (GossipMember remoteMember : remoteList) {
					// Skip myself. We don't want ourselves in the local member list.
					if (!remoteMember.equals(gossipManager.getMyself())) {
						if (gossipManager.getMemberList().contains(remoteMember)) {
							GossipService.debug("The local list already contains the remote member (" + remoteMember + ").");
							// The local memberlist contains the remote member.
							LocalGossipMember localMember = gossipManager.getMemberList().get(gossipManager.getMemberList().indexOf(remoteMember));

							// Let's synchronize it's heartbeat.
							if (remoteMember.getHeartbeat() > localMember.getHeartbeat()) {
								// update local list with latest heartbeat
								localMember.setHeartbeat(remoteMember.getHeartbeat());
								// and reset the timeout of that member
								localMember.resetTimeoutTimer();
							}
							// TODO: Otherwise, should we inform the other when the heartbeat is already higher?
						} else {
							// The local list does not contain the remote member.
							GossipService.debug("The local list does not contain the remote member (" + remoteMember + ").");

							// The remote member is either brand new, or a previously declared dead member.
							// If its dead, check the heartbeat because it may have come back from the dead.
							if (gossipManager.getDeadList().contains(remoteMember)) {
								// The remote member is known here as a dead member.
								GossipService.debug("The remote member is known here as a dead member.");
								LocalGossipMember localDeadMember = gossipManager.getDeadList().get(gossipManager.getDeadList().indexOf(remoteMember));
								// If a member is restarted the heartbeat will restart from 1, so we should check that here.
								// So a member can become from the dead when it is either larger than a previous heartbeat (due to network failure)
								// or when the heartbeat is 1 (after a restart of the service).
								// TODO: What if the first message of a gossip service is sent to a dead node? The second member will receive a heartbeat of two.
								// TODO: The above does happen. Maybe a special message for a revived member?
								// TODO: Or maybe when a member is declared dead for more than _settings.getCleanupInterval() ms, reset the heartbeat to 0.
								// It will then accept a revived member.
								// The above is now handle by checking whether the heartbeat differs _settings.getCleanupInterval(), it must be restarted.
								if (remoteMember.getHeartbeat() == 1 
										|| ((localDeadMember.getHeartbeat() - remoteMember.getHeartbeat()) * -1) >  (gossipManager.getSettings().getCleanupInterval() / 1000)
										|| remoteMember.getHeartbeat() > localDeadMember.getHeartbeat()) {
									GossipService.debug("The remote member is back from the dead. We will remove it from the dead list and add it as a new member.");
									// The remote member is back from the dead.
									// Remove it from the dead list.
									gossipManager.getDeadList().remove(localDeadMember);
									// Add it as a new member and add it to the member list.
									LocalGossipMember newLocalMember = new LocalGossipMember(remoteMember.getHost(), remoteMember.getPort(), remoteMember.getHeartbeat(), gossipManager, gossipManager.getSettings().getCleanupInterval());
									gossipManager.getMemberList().add(newLocalMember);
									newLocalMember.startTimeoutTimer();
									GossipService.info("Removed remote member " + remoteMember.getAddress() + " from dead list and added to local member list.");
								}
							} else {
								// Brand spanking new member - welcome.
								LocalGossipMember newLocalMember = new LocalGossipMember(remoteMember.getHost(), remoteMember.getPort(), remoteMember.getHeartbeat(), gossipManager, gossipManager.getSettings().getCleanupInterval());
								gossipManager.getMemberList().add(newLocalMember);
								newLocalMember.startTimeoutTimer();
								GossipService.info("Added new remote member " + remoteMember.getAddress() + " to local member list.");
							}
						}
					}
				}
			}
		}
	}
	
}
