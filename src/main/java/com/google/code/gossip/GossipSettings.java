package com.google.code.gossip;

/**
 * In this object the settings used by the GossipService are held.
 * 
 * @author harmenw
 */
public class GossipSettings {

	/** Time between gossip'ing in ms. Default is 1 second. */
	private int _gossipInterval = 1000;
	
	/** Time between cleanups in ms. Default is 10 seconds. */
	private int _cleanupInterval = 10000;
	
	/**
	 * Construct GossipSettings with default settings.
	 */
	public GossipSettings() {}
	
	/**
	 * Construct GossipSettings with given settings.
	 * @param gossipInterval The gossip interval in ms.
	 * @param cleanupInterval The cleanup interval in ms.
	 */
	public GossipSettings(int gossipInterval, int cleanupInterval) {
		_gossipInterval = gossipInterval;
		_cleanupInterval = cleanupInterval;
	}
	
	/**
	 * Set the gossip interval.
	 * This is the time between a gossip message is send.
	 * @param gossipInterval The gossip interval in ms.
	 */
	public void setGossipTimeout(int gossipInterval) {
		_gossipInterval = gossipInterval;
	}
	
	/**
	 * Set the cleanup interval.
	 * This is the time between the last heartbeat received from a member and when it will be marked as dead.
	 * @param cleanupInterval The cleanup interval in ms.
	 */
	public void setCleanupInterval(int cleanupInterval) {
		_cleanupInterval = cleanupInterval;
	}
	
	/**
	 * Get the gossip interval.
	 * @return The gossip interval in ms.
	 */
	public int getGossipInterval() {
		return _gossipInterval;
	}
	
	/**
	 * Get the clean interval.
	 * @return The cleanup interval.
	 */
	public int getCleanupInterval() {
		return _cleanupInterval;
	}
}
