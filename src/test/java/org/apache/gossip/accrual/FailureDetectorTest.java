/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.accrual;

import java.net.URI;

import org.apache.gossip.LocalGossipMember;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class FailureDetectorTest {
  
  @Test
  public void aNormalTest(){
    int samples = 1;
    int windowSize = 1000;
    LocalGossipMember member = new LocalGossipMember("", URI.create("udp://127.0.0.1:1000"), 
            "", 0L, null, windowSize, samples, "normal");
    member.recordHeartbeat(5);
    member.recordHeartbeat(10);
    Assert.assertEquals(new Double(0.3010299956639812), member.detect(10));
  }

  @Test
  public void aTest(){
    int samples = 1;
    int windowSize = 1000;
    LocalGossipMember member = new LocalGossipMember("", URI.create("udp://127.0.0.1:1000"), 
            "", 0L, null, windowSize, samples, "exponential");
    member.recordHeartbeat(5);
    member.recordHeartbeat(10);
    Assert.assertEquals(new Double(0.4342944819032518), member.detect(10));
    Assert.assertEquals(new Double(0.5211533782839021), member.detect(11));
    Assert.assertEquals(new Double(1.3028834457097553), member.detect(20));
    Assert.assertEquals(new Double(3.9), member.detect(50), .01);
    Assert.assertEquals(new Double(8.25), member.detect(100), .01);
    Assert.assertEquals(new Double(12.6), member.detect(150), .01);
    Assert.assertEquals(new Double(14.77), member.detect(175), .01);
    Assert.assertEquals(new Double(Double.POSITIVE_INFINITY), member.detect(500), .01);
    member.recordHeartbeat(4);   
    Assert.assertEquals(new Double(12.6), member.detect(150), .01);
  }
  
  @Ignore
  public void sameHeartbeatTest(){
    int samples = 1;
    int windowSize = 1000;
    LocalGossipMember member = new LocalGossipMember("", URI.create("udp://127.0.0.1:1000"), 
            "", 0L, null, windowSize, samples, "exponential");
    member.recordHeartbeat(5);
    member.recordHeartbeat(5);
    member.recordHeartbeat(5);
    Assert.assertEquals(new Double(0.4342944819032518), member.detect(10));
  }
  
}
