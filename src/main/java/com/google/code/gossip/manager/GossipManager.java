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

  private PassiveGossipThread passiveGossipThread;

  private Class<? extends ActiveGossipThread> _activeGossipThreadClass;

  private ActiveGossipThread activeGossipThread;

  public GossipManager(Class<? extends PassiveGossipThread> passiveGossipThreadClass,
          Class<? extends ActiveGossipThread> activeGossipThreadClass, String address, int port,
          String id, GossipSettings settings, ArrayList<GossipMember> gossipMembers) {
    _passiveGossipThreadClass = passiveGossipThreadClass;
    _activeGossipThreadClass = activeGossipThreadClass;
    _settings = settings;
    _me = new LocalGossipMember(address, port, id, 0, this, settings.getCleanupInterval());
    _memberList = new ArrayList<LocalGossipMember>();
    _deadList = new ArrayList<LocalGossipMember>();
    for (GossipMember startupMember : gossipMembers) {
      if (!startupMember.equals(_me)) {
        LocalGossipMember member = new LocalGossipMember(startupMember.getHost(),
                startupMember.getPort(), startupMember.getId(), 0, this,
                settings.getCleanupInterval());
        _memberList.add(member);
        GossipService.LOGGER.debug(member);
      }
    }

    _gossipServiceRunning = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        GossipService.LOGGER.info("Service has been shutdown...");
      }
    }));
  }

  /**
   * All timers associated with a member will trigger this method when it goes off. The timer will
   * go off if we have not heard from this member in <code> _settings.T_CLEANUP </code> time.
   */
  @Override
  public void handleNotification(Notification notification, Object handback) {
    LocalGossipMember deadMember = (LocalGossipMember) notification.getUserData();
    GossipService.LOGGER.info("Dead member detected: " + deadMember);
    synchronized (this._memberList) {
      this._memberList.remove(deadMember);
    }
    synchronized (this._deadList) {
      this._deadList.add(deadMember);
    }
  }

  public GossipSettings getSettings() {
    return _settings;
  }

  /**
   * Get a clone of the memberlist.
   * 
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
   * Starts the client. Specifically, start the various cycles for this protocol. Start the gossip
   * thread and start the receiver thread.
   * 
   * @throws InterruptedException
   */
  public void run() {
    for (LocalGossipMember member : _memberList) {
      if (member != _me) {
        member.startTimeoutTimer();
      }
    }
    _gossipThreadExecutor = Executors.newCachedThreadPool();
    try {
      passiveGossipThread = _passiveGossipThreadClass.getConstructor(GossipManager.class)
              .newInstance(this);
      _gossipThreadExecutor.execute(passiveGossipThread);
      activeGossipThread = _activeGossipThreadClass.getConstructor(GossipManager.class)
              .newInstance(this);
      _gossipThreadExecutor.execute(activeGossipThread);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException e1) {
      throw new RuntimeException(e1);
    }
    GossipService.LOGGER.info("The GossipService is started.");
    while (_gossipServiceRunning.get()) {
      try {
        // TODO
        TimeUnit.MILLISECONDS.sleep(1);
      } catch (InterruptedException e) {
        GossipService.LOGGER.info("The GossipClient was interrupted.");
      }
    }
  }

  /**
   * Shutdown the gossip service.
   */
  public void shutdown() {
    _gossipThreadExecutor.shutdown();
    passiveGossipThread.shutdown();
    activeGossipThread.shutdown();
    try {
      boolean result = _gossipThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
      System.err.println("Terminate retuned " + result);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    _gossipServiceRunning.set(false);
  }
}
