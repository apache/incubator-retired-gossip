package io.teknek.gossip;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LogLevel;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

public class TenNodeThreeSeedTest {


  @Test
  public void test() throws UnknownHostException, InterruptedException{
    abc();
  }

  @Test
  public void testAgain() throws UnknownHostException, InterruptedException{
    abc();
  }

  public void abc() throws InterruptedException, UnknownHostException{
    GossipSettings settings = new GossipSettings();
    int seedNodes = 3;
    List<GossipMember> startupMembers = new ArrayList<>();
    for (int i = 1; i < seedNodes+1; ++i) {
      startupMembers.add(new RemoteGossipMember("127.0.0." + i, 2000, i + ""));
    }
    List<GossipService> clients = new ArrayList<>();
    int clusterMembers = 5;
    for (int i = 1; i < clusterMembers+1; ++i) {
      GossipService gossipService = new GossipService("127.0.0." + i, 2000, i + "", LogLevel.DEBUG,
              startupMembers, settings,
              new GossipListener(){
        @Override
        public void gossipEvent(GossipMember member, GossipState state) {
          System.out.println(member+" "+ state);
        }
      });
      clients.add(gossipService);
      gossipService.start();
      Thread.sleep(1000);
    }
    Thread.sleep(10000);
    for (int i = 0; i < clusterMembers; ++i) {
      Assert.assertEquals(4, clients.get(i).get_gossipManager().getMemberList().size());
    }
    for (int i = 0; i < clusterMembers; ++i) {
      clients.get(i).shutdown();
    }
  }
}
