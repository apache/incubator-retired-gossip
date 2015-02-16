package com.google.code.gossip.event;

import com.google.code.gossip.GossipMember;

public interface GossipListener {
  void gossipEvent(GossipMember member, GossipState state);
}
