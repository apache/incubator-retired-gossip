package com.google.code.gossip;

import javax.management.NotificationListener;

/**
 * This object represent a gossip member with the properties known locally.
 * These objects are stored in the local list of gossip member.s
 * 
 * @author harmenw
 */
public class LocalGossipMember extends GossipMember {
	/** The timeout timer for this gossip member. */
	private transient GossipTimeoutTimer timeoutTimer;

	/**
	 * Constructor.
	 * @param host The hostname or IP address.
	 * @param port The port number.
	 * @param heartbeat The current heartbeat.
	 * @param gossipService The GossipService object.
	 * @param cleanupTimeout The cleanup timeout for this gossip member.
	 */
	public LocalGossipMember(String hostname, int port, int heartbeat, NotificationListener notificationListener, int cleanupTimeout) {
		super(hostname, port, heartbeat);
		
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
