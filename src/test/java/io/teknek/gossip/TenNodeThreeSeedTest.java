package io.teknek.gossip;

import io.teknek.tunit.TUnit;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

public class TenNodeThreeSeedTest {
  private static final Logger log = Logger.getLogger( TenNodeThreeSeedTest.class );

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

    log.info( "Adding seed nodes" );
    int seedNodes = 3;
    List<GossipMember> startupMembers = new ArrayList<>();
    for (int i = 1; i < seedNodes+1; ++i) {
      startupMembers.add(new RemoteGossipMember("127.0.0.1", 50000 + i, i + ""));
    }

    log.info( "Adding clients" );
    final List<GossipService> clients = new ArrayList<>();
    final int clusterMembers = 5;
    for (int i = 1; i < clusterMembers+1; ++i) {
      GossipService gossipService = new GossipService("127.0.0.1", 50000 + i, i + "",
              startupMembers, settings,
              new GossipListener(){
        @Override
        public void gossipEvent(GossipMember member, GossipState state) {
          log.info(member+" "+ state);
        }
      });
      clients.add(gossipService);
      gossipService.start();
      gossipService.get_gossipManager().getMemberList();
    }
    TUnit.assertThat(new Callable<Integer> (){
      public Integer call() throws Exception {
        int total = 0;
        for (int i = 0; i < clusterMembers; ++i) {
          total += clients.get(i).get_gossipManager().getMemberList().size();
        }
        return total;
      }}).afterWaitingAtMost(10, TimeUnit.SECONDS).isEqualTo(20);
    
    for (int i = 0; i < clusterMembers; ++i) {
      clients.get(i).shutdown();
    }
  }
}
