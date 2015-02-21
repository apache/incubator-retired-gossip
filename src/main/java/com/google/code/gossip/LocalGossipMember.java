package com.google.code.gossip;

import javax.management.NotificationListener;

/**
 * This object represent a gossip member with the properties known locally. These objects are stored
 * in the local list of gossip member.s
 *
 * @author harmenw
 */
public class LocalGossipMember extends GossipMember {
  /** The timeout timer for this gossip member. */
  private final transient GossipTimeoutTimer timeoutTimer;

  /**
   * Constructor.
   *
   * @param hostname
   *          The hostname or IP address.
   * @param port
   *          The port number.
   * @param id
   * @param heartbeat
   *          The current heartbeat.
   * @param notificationListener
   * @param cleanupTimeout
   *          The cleanup timeout for this gossip member.
   */
  public LocalGossipMember(String hostname, int port, String id, int heartbeat,
          NotificationListener notificationListener, int cleanupTimeout) {
    super(hostname, port, id, heartbeat);

    this.timeoutTimer = new GossipTimeoutTimer(cleanupTimeout, notificationListener, this);
  }

  /**
   * Start the timeout timer.
   */
  public void startTimeoutTimer() {
    this.timeoutTimer.start();
  }

  /**
   * Reset the timeout timer.
   */
  public void resetTimeoutTimer() {
    this.timeoutTimer.reset();
  }
}
