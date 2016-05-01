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
package com.google.code.gossip;

import java.util.Date;

import javax.management.NotificationListener;
import javax.management.timer.Timer;

/**
 * This object represents a timer for a gossip member. When the timer has elapsed without being
 * reset in the meantime, it will inform the GossipService about this who in turn will put the
 * gossip member on the dead list, because it is apparantly not alive anymore.
 * 
 * @author joshclemm, harmenw
 */
public class GossipTimeoutTimer extends Timer {

  private final long sleepTime;

  private final LocalGossipMember source;

  /**
   * Constructor. Creates a reset-able timer that wakes up after millisecondsSleepTime.
   * 
   * @param millisecondsSleepTime
   *          The time for this timer to wait before an event.
   * @param notificationListener
   * @param member
   */
  public GossipTimeoutTimer(long millisecondsSleepTime, NotificationListener notificationListener,
          LocalGossipMember member) {
    super();
    sleepTime = millisecondsSleepTime;
    source = member;
    addNotificationListener(notificationListener, null, null);
  }

  /**
   * @see javax.management.timer.Timer#start()
   */
  public void start() {
    this.reset();
    super.start();
  }

  /**
   * Resets timer to start counting down from original time.
   */
  public void reset() {
    removeAllNotifications();
    setWakeupTime(sleepTime);
  }

  /**
   * Adds a new wake-up time for this timer.
   * 
   * @param milliseconds
   */
  private void setWakeupTime(long milliseconds) {
    addNotification("type", "message", source, new Date(System.currentTimeMillis() + milliseconds));
  }
}
