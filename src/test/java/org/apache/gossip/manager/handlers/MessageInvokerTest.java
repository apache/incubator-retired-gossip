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
package org.apache.gossip.manager.handlers;

import org.apache.gossip.manager.GossipCore;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.ActiveGossipMessage;
import org.apache.gossip.model.Base;
import org.apache.gossip.udp.UdpSharedGossipDataMessage;
import org.junit.Assert;
import org.junit.Test;

public class MessageInvokerTest {
  private class FakeMessage extends Base {
    public FakeMessage() {
    }
  }

  private class FakeMessageData extends Base {
    public int data;

    public FakeMessageData(int data) {
      this.data = data;
    }
  }

  private class FakeMessageDataHandler implements MessageHandler {
    public int data;

    public FakeMessageDataHandler() {
      data = 0;
    }

    public void invoke(GossipCore gossipCore, GossipManager gossipManager, Base base) {
      data = ((FakeMessageData) base).data;
    }
  }

  private class FakeMessageHandler implements MessageHandler {
    public int counter;

    public FakeMessageHandler() {
      counter = 0;
    }

    public void invoke(GossipCore gossipCore, GossipManager gossipManager, Base base) {
      counter++;
    }
  }

  @Test
  public void testSimpleInvoker() {
    MessageInvoker mi = new SimpleMessageInvoker(FakeMessage.class, new FakeMessageHandler());
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertFalse(mi.invoke(null, null, new ActiveGossipMessage()));
  }

  @Test(expected = NullPointerException.class)
  public void testSimpleInvokerNullClassConstructor() {
    new SimpleMessageInvoker(null, new FakeMessageHandler());
  }

  @Test(expected = NullPointerException.class)
  public void testSimpleInvokerNullHandlerConstructor() {
    new SimpleMessageInvoker(FakeMessage.class, null);
  }

  @Test
  public void testCallCountSimpleInvoker() {
    FakeMessageHandler h = new FakeMessageHandler();
    MessageInvoker mi = new SimpleMessageInvoker(FakeMessage.class, h);
    mi.invoke(null, null, new FakeMessage());
    Assert.assertEquals(1, h.counter);
    mi.invoke(null, null, new ActiveGossipMessage());
    Assert.assertEquals(1, h.counter);
    mi.invoke(null, null, new FakeMessage());
    Assert.assertEquals(2, h.counter);
  }

  @Test(expected = NullPointerException.class)
  public void cantAddNullInvoker() {
    MessageInvokerCombiner mi = new MessageInvokerCombiner();
    mi.add(null);
  }

  @Test
  public void testCombinerClear() {
    MessageInvokerCombiner mi = new MessageInvokerCombiner();
    mi.add(new SimpleMessageInvoker(FakeMessage.class, new FakeMessageHandler()));
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));

    mi.clear();
    Assert.assertFalse(mi.invoke(null, null, new FakeMessage()));
  }

  @Test
  public void testMessageInvokerCombiner() {
    //Empty combiner - false result
    MessageInvokerCombiner mi = new MessageInvokerCombiner();
    Assert.assertFalse(mi.invoke(null, null, new Base()));

    FakeMessageHandler h = new FakeMessageHandler();
    mi.add(new SimpleMessageInvoker(FakeMessage.class, h));
    mi.add(new SimpleMessageInvoker(FakeMessage.class, h));

    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertFalse(mi.invoke(null, null, new ActiveGossipMessage()));
    Assert.assertEquals(2, h.counter);

    //Increase size in runtime. Should be 3 calls: 2+3 = 5
    mi.add(new SimpleMessageInvoker(FakeMessage.class, h));
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertEquals(5, h.counter);
  }

  @Test
  public void testMessageInvokerCombiner2levels() {
    MessageInvokerCombiner mi = new MessageInvokerCombiner();
    FakeMessageHandler h = new FakeMessageHandler();

    MessageInvokerCombiner mi1 = new MessageInvokerCombiner();
    mi1.add(new SimpleMessageInvoker(FakeMessage.class, h));
    mi1.add(new SimpleMessageInvoker(FakeMessage.class, h));

    MessageInvokerCombiner mi2 = new MessageInvokerCombiner();
    mi2.add(new SimpleMessageInvoker(FakeMessage.class, h));
    mi2.add(new SimpleMessageInvoker(FakeMessage.class, h));

    mi.add(mi1);
    mi.add(mi2);

    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
    Assert.assertEquals(4, h.counter);
  }

  @Test
  public void testMessageInvokerCombinerDataShipping() {
    MessageInvokerCombiner mi = new MessageInvokerCombiner();
    FakeMessageDataHandler h = new FakeMessageDataHandler();
    mi.add(new SimpleMessageInvoker(FakeMessageData.class, h));

    Assert.assertTrue(mi.invoke(null, null, new FakeMessageData(101)));
    Assert.assertEquals(101, h.data);
  }

  @Test
  public void testCombiningDefaultInvoker() {
    MessageInvokerCombiner mi = new MessageInvokerCombiner();
    mi.add(new DefaultMessageInvoker());
    mi.add(new SimpleMessageInvoker(FakeMessage.class, new FakeMessageHandler()));
    //UdpSharedGossipDataMessage with null gossipCore -> exception
    boolean thrown = false;
    try {
      mi.invoke(null, null, new UdpSharedGossipDataMessage());
    } catch (NullPointerException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
    //DefaultInvoker skips FakeMessage and FakeHandler works ok
    Assert.assertTrue(mi.invoke(null, null, new FakeMessage()));
  }

}
