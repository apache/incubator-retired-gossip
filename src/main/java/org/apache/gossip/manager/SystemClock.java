package org.apache.gossip.manager;

public class SystemClock implements Clock {

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public long nanoTime() {
    return System.nanoTime();
  }

}
