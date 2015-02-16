package com.google.code.gossip.event;

public enum GossipState {
  UP("up"), DOWN("down");
  private String state;
  
  private GossipState(String state){
    this.state = state;
  }
}
