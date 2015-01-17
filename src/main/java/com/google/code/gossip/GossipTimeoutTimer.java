package com.google.code.gossip;

import java.util.Date;

import javax.management.NotificationListener;
import javax.management.timer.Timer;

/**
 * This object represents a timer for a gossip member.
 * When the timer has elapsed without being reset in the meantime, it will inform the GossipService about this
 * who in turn will put the gossip member on the dead list, because it is apparantly not alive anymore.
 * 
 * @author joshclemm, harmenw
 */
public class GossipTimeoutTimer extends Timer {

	/** The amount of time this timer waits before generating a wake-up event. */
	private long _sleepTime;

	/** The gossip member this timer is for. */
	private LocalGossipMember _source;

	/**
	 * Constructor.
	 * Creates a reset-able timer that wakes up after millisecondsSleepTime.
	 * @param millisecondsSleepTime The time for this timer to wait before an event.
	 * @param service
	 * @param member
	 */
	public GossipTimeoutTimer(long millisecondsSleepTime, NotificationListener notificationListener, LocalGossipMember member) {
		super();
		_sleepTime = millisecondsSleepTime;
		_source = member;
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
		setWakeupTime(_sleepTime);
	}

	/**
	 * Adds a new wake-up time for this timer.
	 * @param milliseconds
	 */
	private void setWakeupTime(long milliseconds) {
		addNotification("type", "message", _source, new Date(System.currentTimeMillis()+milliseconds));
	}
}

