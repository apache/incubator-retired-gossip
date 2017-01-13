package org.apache.gossip;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class GossipMemberTest {

  @Test
  public void testHashCodeFromGossip40() throws URISyntaxException {
    Assert.assertNotEquals(
            new LocalGossipMember("mycluster", new URI("udp://4.4.4.4:1000"), "myid", 1, 10, 5)
                    .hashCode(),
            new LocalGossipMember("mycluster", new URI("udp://4.4.4.5:1005"), "yourid", 11, 11, 6)
                    .hashCode());
  }
}
