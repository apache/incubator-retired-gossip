package io.teknek.gossip;

import io.teknek.tunit.TUnit;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

public class ShutdownDeadtimeTest {

  private static final Logger log = Logger.getLogger(ShutdownDeadtimeTest.class );
  @Test
  //@Ignore
  public void DeadNodesDoNotComeAliveAgain() throws InterruptedException, UnknownHostException {
      GossipSettings settings = new GossipSettings(1000, 10000);
      
      log.info( "Adding seed nodes" );
      int seedNodes = 3;
      List<GossipMember> startupMembers = new ArrayList<>();
      for (int i = 1; i < seedNodes + 1; ++i) {
          startupMembers.add(new RemoteGossipMember("127.0.0.1", 50000 + i, i + ""));
      }

      log.info( "Adding clients" );
      final List<GossipService> clients = new ArrayList<>();
      final int clusterMembers = 5;
      for (int i = 1; i < clusterMembers+1; ++i) {
          final int j = i;
          GossipService gossipService = new GossipService("127.0.0.1", 50000 + i, i + "",
                  startupMembers, settings,
                  new GossipListener(){
                      @Override
                      public void gossipEvent(GossipMember member, GossipState state) {
                          System.out.println(System.currentTimeMillis() + " Member "+j + " reports "+ member+" "+ state);
                      }
                  });
          clients.add(gossipService);
          gossipService.start();
      }
      TUnit.assertThat(new Callable<Integer> (){
          public Integer call() throws Exception {
              int total = 0;
              for (int i = 0; i < clusterMembers; ++i) {
                  total += clients.get(i).get_gossipManager().getMemberList().size();
              }
              return total;
          }}).afterWaitingAtMost(20, TimeUnit.SECONDS).isEqualTo(20);

      // shutdown one client and verify that one client is lost.
      Random r = new Random();
      int randomClientId = r.nextInt(clusterMembers);
      log.info( "shutting down " + randomClientId );
      final int shutdownPort = clients.get(randomClientId).get_gossipManager().getMyself().getPort();
      final String shutdownId = clients.get(randomClientId).get_gossipManager().getMyself().getId();
      clients.get(randomClientId).shutdown();
      TUnit.assertThat(new Callable<Integer> (){
          public Integer call() throws Exception {
              int total = 0;
              for (int i = 0; i < clusterMembers; ++i) {
                  total += clients.get(i).get_gossipManager().getMemberList().size();
              }
              return total;
          }}).afterWaitingAtMost(20, TimeUnit.SECONDS).isEqualTo(16);
      clients.remove(randomClientId);
      
      TUnit.assertThat(new Callable<Integer> (){
        public Integer call() throws Exception {
            int total = 0;
            for (int i = 0; i < clusterMembers - 1; ++i) {
                total += clients.get(i).get_gossipManager().getDeadList().size();
            }
            return total;
        }}).afterWaitingAtMost(10, TimeUnit.SECONDS).isEqualTo(4);
      
      // start client again
      GossipService gossipService = new GossipService("127.0.0.1", shutdownPort, shutdownId + "",
              startupMembers, settings,
              new GossipListener(){
                  @Override
                  public void gossipEvent(GossipMember member, GossipState state) {
                      //System.out.println("revived " + member+" "+ state);
                  }
              });
      clients.add(gossipService);
      gossipService.start();

      // verify that the client is alive again for every node
      TUnit.assertThat(new Callable<Integer> (){
          public Integer call() throws Exception {
              int total = 0;
              for (int i = 0; i < clusterMembers; ++i) {
                  total += clients.get(i).get_gossipManager().getMemberList().size();
              }
              return total;
          }}).afterWaitingAtMost(20, TimeUnit.SECONDS).isEqualTo(20);
      
      for (int i = 0; i < clusterMembers; ++i) {
        clients.get(i).shutdown();
      }
  }
}
