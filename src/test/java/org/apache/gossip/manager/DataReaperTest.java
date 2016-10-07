package org.apache.gossip.manager;

import java.net.URI;

import org.apache.gossip.GossipSettings;
import org.apache.gossip.manager.random.RandomGossipManager;
import org.apache.gossip.model.GossipDataMessage;
import org.apache.gossip.model.SharedGossipDataMessage;
import org.junit.Assert;
import org.junit.Test;

import io.teknek.tunit.TUnit;

public class DataReaperTest {

  @Test
  public void testReaperOneShot() {
    String myId = "4";
    String key = "key";
    String value = "a";
    GossipSettings settings = new GossipSettings();
    GossipManager gm = RandomGossipManager.newBuilder().cluster("abc").settings(settings)
            .withId(myId).uri(URI.create("udp://localhost:5000")).build();
    gm.gossipPerNodeData(perNodeDatum(key, value));
    gm.gossipSharedData(sharedDatum(key, value));
    Assert.assertEquals(value, gm.findPerNodeGossipData(myId, key).getPayload());
    Assert.assertEquals(value, gm.findSharedGossipData(key).getPayload());
    gm.getDataReaper().runPerNodeOnce();
    gm.getDataReaper().runSharedOnce();
    TUnit.assertThat(() -> gm.findPerNodeGossipData(myId, key)).equals(null);
    TUnit.assertThat(() -> gm.findSharedGossipData(key)).equals(null);
  }

  private GossipDataMessage perNodeDatum(String key, String value) {
    GossipDataMessage m = new GossipDataMessage();
    m.setExpireAt(System.currentTimeMillis() + 5L);
    m.setKey(key);
    m.setPayload(value);
    m.setTimestamp(System.currentTimeMillis());
    return m;
  }
  
  private SharedGossipDataMessage sharedDatum(String key, String value) {
    SharedGossipDataMessage m = new SharedGossipDataMessage();
    m.setExpireAt(System.currentTimeMillis() + 5L);
    m.setKey(key);
    m.setPayload(value);
    m.setTimestamp(System.currentTimeMillis());
    return m;
  }
  

  @Test
  public void testHigherTimestampWins() {
    String myId = "4";
    String key = "key";
    String value = "a";
    GossipSettings settings = new GossipSettings();
    GossipManager gm = RandomGossipManager.newBuilder().cluster("abc").settings(settings)
            .withId(myId).uri(URI.create("udp://localhost:5000")).build();
    GossipDataMessage before = perNodeDatum(key, value);
    GossipDataMessage after = perNodeDatum(key, "b");
    after.setTimestamp(after.getTimestamp() - 1);
    gm.gossipPerNodeData(before);
    Assert.assertEquals(value, gm.findPerNodeGossipData(myId, key).getPayload());
    gm.gossipPerNodeData(after);
    Assert.assertEquals(value, gm.findPerNodeGossipData(myId, key).getPayload());
  }

}
