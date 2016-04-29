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

/**
 * In this object the settings used by the GossipService are held.
 * 
 * @author harmenw
 */
public class GossipSettings {

  /** Time between gossip'ing in ms. Default is 1 second. */
  private int gossipInterval = 1000;

  /** Time between cleanups in ms. Default is 10 seconds. */
  private int cleanupInterval = 10000;

  /**
   * Construct GossipSettings with default settings.
   */
  public GossipSettings() {
  }

  /**
   * Construct GossipSettings with given settings.
   * 
   * @param gossipInterval
   *          The gossip interval in ms.
   * @param cleanupInterval
   *          The cleanup interval in ms.
   */
  public GossipSettings(int gossipInterval, int cleanupInterval) {
    this.gossipInterval = gossipInterval;
    this.cleanupInterval = cleanupInterval;
  }

  /**
   * Set the gossip interval. This is the time between a gossip message is send.
   * 
   * @param gossipInterval
   *          The gossip interval in ms.
   */
  public void setGossipTimeout(int gossipInterval) {
    this.gossipInterval = gossipInterval;
  }

  /**
   * Set the cleanup interval. This is the time between the last heartbeat received from a member
   * and when it will be marked as dead.
   * 
   * @param cleanupInterval
   *          The cleanup interval in ms.
   */
  public void setCleanupInterval(int cleanupInterval) {
    this.cleanupInterval = cleanupInterval;
  }

  /**
   * Get the gossip interval.
   * 
   * @return The gossip interval in ms.
   */
  public int getGossipInterval() {
    return gossipInterval;
  }

  /**
   * Get the clean interval.
   * 
   * @return The cleanup interval.
   */
  public int getCleanupInterval() {
    return cleanupInterval;
  }
}
