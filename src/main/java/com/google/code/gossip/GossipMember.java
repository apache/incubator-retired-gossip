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

import java.net.InetSocketAddress;

/**
 * A abstract class representing a gossip member.
 * 
 * @author joshclemm, harmenw
 */
public abstract class GossipMember implements Comparable<GossipMember> {

  
  protected final String host;

  protected final int port;

  protected volatile long heartbeat;

  protected final String clusterName;

  /**
   * The purpose of the id field is to be able for nodes to identify themselves beyond there
   * host/port. For example an application might generate a persistent id so if they rejoin the
   * cluster at a different host and port we are aware it is the same node.
   */
  protected String id;

  /**
   * Constructor.
   * 
   * @param host
   *          The hostname or IP address.
   * @param port
   *          The port number.
   * @param heartbeat
   *          The current heartbeat.
   * @param id
   *          an id that may be replaced after contact
   */
  public GossipMember(String clusterName, String host, int port, String id, long heartbeat) {
    this.clusterName = clusterName;
    this.host = host;
    this.port = port;
    this.id = id;
    this.heartbeat = heartbeat;
  }

  /**
   * Get the name of the cluster the member belongs to.
   * 
   * @return The cluster name
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * Get the hostname or IP address of the remote gossip member.
   * 
   * @return The hostname or IP address.
   */
  public String getHost() {
    return host;
  }

  /**
   * Get the port number of the remote gossip member.
   * 
   * @return The port number.
   */
  public int getPort() {
    return port;
  }

  /**
   * The member address in the form IP/host:port Similar to the toString in
   * {@link InetSocketAddress}
   */
  public String getAddress() {
    return host + ":" + port;
  }

  /**
   * Get the heartbeat of this gossip member.
   * 
   * @return The current heartbeat.
   */
  public long getHeartbeat() {
    return heartbeat;
  }

  /**
   * Set the heartbeat of this gossip member.
   * 
   * @param heartbeat
   *          The new heartbeat.
   */
  public void setHeartbeat(long heartbeat) {
    this.heartbeat = heartbeat;
  }

  public String getId() {
    return id;
  }

  public void setId(String _id) {
    this.id = _id;
  }

  public String toString() {
    return "Member [address=" + getAddress() + ", id=" + id + ", heartbeat=" + heartbeat + "]";
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    String address = getAddress();
    result = prime * result + ((address == null) ? 0 : address.hashCode()) + clusterName == null ? 0
            : clusterName.hashCode();
    return result;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      System.err.println("equals(): obj is null.");
      return false;
    }
    if (!(obj instanceof GossipMember)) {
      System.err.println("equals(): obj is not of type GossipMember.");
      return false;
    }
    // The object is the same of they both have the same address (hostname and port).
    return getAddress().equals(((LocalGossipMember) obj).getAddress())
            && getClusterName().equals(((LocalGossipMember) obj).getClusterName());
  }

  public int compareTo(GossipMember other) {
    return this.getAddress().compareTo(other.getAddress());
  }
}
