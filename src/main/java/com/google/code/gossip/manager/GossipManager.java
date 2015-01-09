package com.google.code.gossip.manager;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;
import javax.management.NotificationListener;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;

public abstract class GossipManager extends Thread implements NotificationListener {
	/** The maximal number of bytes the packet with the GOSSIP may be. (Default is 100 kb) */
	public static final int MAX_PACKET_SIZE = 102400;
	
	/** The list of members which are in the gossip group (not including myself). */
	private ArrayList<LocalGossipMember> _memberList;

	/** The list of members which are known to be dead. */
	private ArrayList<LocalGossipMember> _deadList;
	
	/** The member I am representing. */
	private LocalGossipMember _me;
	
	/** The settings for gossiping. */
	private GossipSettings _settings;
	
	/** A boolean whether the gossip service should keep running. */
	private AtomicBoolean _gossipServiceRunning;

	/** A ExecutorService used for executing the active and passive gossip threads. */
	private ExecutorService _gossipThreadExecutor;
	
	private Class<? extends PassiveGossipThread> _passiveGossipThreadClass;
	
	private Class<? extends ActiveGossipThread> _activeGossipThreadClass;
	
	public GossipManager(Class<? extends PassiveGossipThread> passiveGossipThreadClass, Class<? extends ActiveGossipThread> activeGossipThreadClass, String address, int port, GossipSettings settings, ArrayList<GossipMember> gossipMembers) {
		// Set the active and passive gossip thread classes to use.
		_passiveGossipThreadClass = passiveGossipThreadClass;
		_activeGossipThreadClass = activeGossipThreadClass;
		
		// Assign the GossipSettings to the instance variable.
		_settings = settings;
		
		// Create the local gossip member which I am representing.
		_me = new LocalGossipMember(address, port, 0, this, settings.getCleanupInterval());
		
		// Initialize the gossip members list.
		_memberList = new ArrayList<LocalGossipMember>();
		
		// Initialize the dead gossip members list.
		_deadList = new ArrayList<LocalGossipMember>();
		
		// Print the startup member list when the service is in debug mode.
		GossipService.debug("Startup member list:");
		GossipService.debug("---------------------");
		// First print out myself.
		GossipService.debug(_me);
		// Copy the list with members given to the local member list and print the member when in debug mode.
		for (GossipMember startupMember : gossipMembers) {
			if (!startupMember.equals(_me)) {
				LocalGossipMember member = new LocalGossipMember(startupMember.getHost(), startupMember.getPort(), 0, this, settings.getCleanupInterval());
				_memberList.add(member);
				GossipService.debug(member);
			} else {
				GossipService.info("Found myself in the members section of the configuration, you should not add the host itself to the members section.");
			}
		}
		
		// Set the boolean for running the gossip service to true.
		_gossipServiceRunning = new AtomicBoolean(true);
		
		// Add a shutdown hook so we can see when the service has been shutdown.
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				GossipService.info("Service has been shutdown...");
			}
		}));
	}
	
	/**
	 * All timers associated with a member will trigger this method when it goes
	 * off. The timer will go off if we have not heard from this member in
	 * <code> _settings.T_CLEANUP </code> time.
	 */
	@Override
	public void handleNotification(Notification notification, Object handback) {

		// Get the local gossip member associated with the notification.
		LocalGossipMember deadMember = (LocalGossipMember) notification.getUserData();

		GossipService.info("Dead member detected: " + deadMember);

		// Remove the member from the active member list.
		synchronized (this._memberList) {
			this._memberList.remove(deadMember);
		}

		// Add the member to the dead member list.
		synchronized (this._deadList) {
			this._deadList.add(deadMember);
		}
	}

	public GossipSettings getSettings() {
		return _settings;
	}

	/**
	 * Get a clone of the memberlist.
	 * @return
	 */
	public ArrayList<LocalGossipMember> getMemberList() {
		return _memberList;
	}
	
	public LocalGossipMember getMyself() {
		return _me;
	}

	public ArrayList<LocalGossipMember> getDeadList() {
		return _deadList;
	}
	
	/**
	 * Starts the client.  Specifically, start the various cycles for this protocol.
	 * Start the gossip thread and start the receiver thread.
	 * @throws InterruptedException
	 */
	public void run() {
		// Start all timers except for me
		for (LocalGossipMember member : _memberList) {
			if (member != _me) {
				member.startTimeoutTimer();
			}
		}

		try {
			_gossipThreadExecutor = Executors.newCachedThreadPool();
			//  The receiver thread is a passive player that handles
			//  merging incoming membership lists from other neighbors.
			_gossipThreadExecutor.execute(_passiveGossipThreadClass.getConstructor(GossipManager.class).newInstance(this));
			//  The gossiper thread is an active player that 
			//  selects a neighbor to share its membership list
			_gossipThreadExecutor.execute(_activeGossipThreadClass.getConstructor(GossipManager.class).newInstance(this));
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		} catch (InvocationTargetException e1) {
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}

		// Potentially, you could kick off more threads here
		//  that could perform additional data synching
		
		GossipService.info("The GossipService is started.");

		// keep the main thread around
		while(_gossipServiceRunning.get()) {
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				GossipService.info("The GossipClient was interrupted.");
			}
		}
	}
	
	/**
	 * Shutdown the gossip service.
	 */
	public void shutdown() {
		_gossipThreadExecutor.shutdown();
		_gossipServiceRunning.set(false);
	}
}
