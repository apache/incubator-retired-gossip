package io.teknek.gossip;

import java.net.UnknownHostException;
import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LogLevel;
import com.google.code.gossip.RemoteGossipMember;

public class TenNodeThreeSeedTest {

  
  @Test
  public void test() throws UnknownHostException, InterruptedException{
    GossipSettings settings = new GossipSettings();
    
    int seedNodes = 3;
    ArrayList<GossipMember> startupMembers = new ArrayList<GossipMember>();
    for (int i = 1; i < seedNodes+1; ++i) {
      startupMembers.add(new RemoteGossipMember("127.0.0." + i, 2000));
    }
    
    ArrayList<GossipService> clients = new ArrayList<GossipService>();
    int clusterMembers = 10;
    for (int i = 1; i < clusterMembers+1; ++i) {
      GossipService gossipService = new GossipService("127.0.0."+i, 2000, LogLevel.DEBUG, startupMembers, settings);
      clients.add(gossipService);
      gossipService.start();
      Thread.sleep(settings.getCleanupInterval() + 1000);
    }
    Thread.sleep(10000);
    for (int i = 0; i < clusterMembers; ++i) {
      Assert.assertEquals(9, clients.get(i).get_gossipManager().getMemberList().size());
    }
    for (int i = 0; i < clusterMembers; ++i) {
      clients.get(i).shutdown();
    }
  }
}
