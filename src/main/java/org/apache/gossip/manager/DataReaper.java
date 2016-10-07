package org.apache.gossip.manager;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.gossip.model.GossipDataMessage;
import org.apache.gossip.model.SharedGossipDataMessage;

/**
 * We wish to periodically sweep user data and remove entries past their timestamp. This
 * implementation periodically sweeps through the data and removes old entries. While it might make
 * sense to use a more specific high performance data-structure to handle eviction, keep in mind
 * that we are not looking to store a large quantity of data as we currently have to transmit this
 * data cluster wide.
 */
public class DataReaper {

  private final GossipCore gossipCore;
  private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
  private final Clock clock;
  
  public DataReaper(GossipCore gossipCore, Clock clock){
    this.gossipCore = gossipCore;
    this.clock = clock;
  }
  
  public void init(){
    Runnable reapPerNodeData = () -> {
      runPerNodeOnce();
      runSharedOnce();
    };
    scheduledExecutor.scheduleAtFixedRate(reapPerNodeData, 0, 5, TimeUnit.SECONDS);
  }
  
  void runSharedOnce(){
    for (Entry<String, SharedGossipDataMessage> entry : gossipCore.getSharedData().entrySet()){
      if (entry.getValue().getExpireAt() < clock.currentTimeMillis()){
        gossipCore.getSharedData().remove(entry.getKey(), entry.getValue());
      }
    }
  }
  
  void runPerNodeOnce(){
    for (Entry<String, ConcurrentHashMap<String, GossipDataMessage>> node : gossipCore.getPerNodeData().entrySet()){
      reapData(node.getValue());
    }
  }
  
  void reapData(ConcurrentHashMap<String, GossipDataMessage> concurrentHashMap){
    for (Entry<String, GossipDataMessage> entry : concurrentHashMap.entrySet()){
      if (entry.getValue().getExpireAt() < clock.currentTimeMillis()){
        concurrentHashMap.remove(entry.getKey(), entry.getValue());
      }
    }
  }
  
  public void close(){
    scheduledExecutor.shutdown();
    try {
      scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
  }
}
