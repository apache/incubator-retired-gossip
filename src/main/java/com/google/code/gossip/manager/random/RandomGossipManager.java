package com.google.code.gossip.manager.random;

import java.util.ArrayList;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.impl.OnlyProcessReceivedPassiveGossipThread;

public class RandomGossipManager extends GossipManager {
	public RandomGossipManager(String address, int port, GossipSettings settings, ArrayList<GossipMember> gossipMembers) {
		super(OnlyProcessReceivedPassiveGossipThread.class, RandomActiveGossipThread.class, address, port, settings, gossipMembers);
	}
}
