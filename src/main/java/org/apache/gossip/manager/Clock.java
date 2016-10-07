package org.apache.gossip.manager;

public interface Clock {

  long currentTimeMillis();
  long nanoTime();
  
}
