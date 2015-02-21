package com.google.code.gossip;

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
  public RemoteGossipMember(String hostname, int port, String id, int heartbeat) {
    super(hostname, port, id, heartbeat);
  }

  /**
   * Construct a RemoteGossipMember with a heartbeat of 0.
   *
   * @param hostname
   *          The hostname or IP address.
   * @param port
   *          The port number.
   */
  public RemoteGossipMember(String hostname, int port, String id) {
    super(hostname, port, id, 0);
  }
}
