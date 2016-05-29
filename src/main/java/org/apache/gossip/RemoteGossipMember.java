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
package org.apache.gossip;

/**
 * The object represents a gossip member with the properties as received from a remote gossip
 * member.
 * 
 * @author harmenw
 */
public class RemoteGossipMember extends GossipMember {

  /**
   * Constructor.
   * 
   * @param hostname
   *          The hostname or IP address.
   * @param port
   *          The port number.
   * @param heartbeat
   *          The current heartbeat.
   */
  public RemoteGossipMember(String clusterName, String hostname, int port, String id, long heartbeat) {
    super(clusterName, hostname, port, id, heartbeat);
  }

  /**
   * Construct a RemoteGossipMember with a heartbeat of 0.
   * 
   * @param hostname
   *          The hostname or IP address.
   * @param port
   *          The port number.
   */
  public RemoteGossipMember(String clusterName, String hostname, int port, String id) {
    super(clusterName, hostname, port, id, System.currentTimeMillis());
  }
}
