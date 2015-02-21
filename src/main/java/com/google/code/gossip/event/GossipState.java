package com.google.code.gossip.event;

public enum GossipState {
  UP("up"), DOWN("down");
  private final String state;

  private GossipState(String state){
    this.state = state;
  }
}
