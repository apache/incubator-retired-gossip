package com.google.code.gossip.manager;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.log4j.Logger;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

public abstract class GossipManager extends Thread implements NotificationListener {

  public static final Logger LOGGER = Logger.getLogger(GossipManager.class);
  public static final int MAX_PACKET_SIZE = 102400;

  private final ConcurrentSkipListMap<LocalGossipMember,GossipState> members;
  private final LocalGossipMember _me;
  private final GossipSettings _settings;
  private final AtomicBoolean _gossipServiceRunning;
  private final Class<? extends PassiveGossipThread> _passiveGossipThreadClass;
  private final Class<? extends ActiveGossipThread> _activeGossipThreadClass;
  private final GossipListener listener;
  private ActiveGossipThread activeGossipThread;
  private PassiveGossipThread passiveGossipThread;
  private ExecutorService _gossipThreadExecutor;

  public GossipManager(Class<? extends PassiveGossipThread> passiveGossipThreadClass,
          Class<? extends ActiveGossipThread> activeGossipThreadClass, String address, int port,
          String id, GossipSettings settings, List<GossipMember> gossipMembers,
          GossipListener listener) {
    _passiveGossipThreadClass = passiveGossipThreadClass;
    _activeGossipThreadClass = activeGossipThreadClass;
    _settings = settings;
    _me = new LocalGossipMember(address, port, id, 0, this, settings.getCleanupInterval());
    members = new ConcurrentSkipListMap<>();
    for (GossipMember startupMember : gossipMembers) {
      if (!startupMember.equals(_me)) {
        LocalGossipMember member = new LocalGossipMember(startupMember.getHost(),
                startupMember.getPort(), startupMember.getId(), 0, this,
                settings.getCleanupInterval());
        members.put(member, GossipState.UP);
        GossipService.LOGGER.debug(member);
      }
    }

    _gossipServiceRunning = new AtomicBoolean(true);
    this.listener = listener;
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
    members.put(deadMember, GossipState.DOWN);
    if (listener != null) {
      listener.gossipEvent(deadMember, GossipState.DOWN);
    }
  }

  public void createOrRevivieMember(LocalGossipMember m){
    members.put(m, GossipState.UP);
    if (listener != null) {
      listener.gossipEvent(m, GossipState.UP);
    }
  }

  public GossipSettings getSettings() {
    return _settings;
  }

  public List<LocalGossipMember> getMemberList() {
    List<LocalGossipMember> up = new ArrayList<>();
    for (Entry<LocalGossipMember, GossipState> entry : members.entrySet()){
      if (GossipState.UP.equals(entry.getValue())){
        up.add(entry.getKey());
      }
    }
    return Collections.unmodifiableList(up);
  }

  public LocalGossipMember getMyself() {
    return _me;
  }

  public List<LocalGossipMember> getDeadList() {
    List<LocalGossipMember> up = new ArrayList<>();
    for (Entry<LocalGossipMember, GossipState> entry : members.entrySet()){
      if (GossipState.DOWN.equals(entry.getValue())){
        up.add(entry.getKey());
      }
    }
    return Collections.unmodifiableList(up);
  }

  /**
   * Starts the client. Specifically, start the various cycles for this protocol. Start the gossip
   * thread and start the receiver thread.
   */
  public void run() {
    for (LocalGossipMember member : members.keySet()) {
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
      if (!result){
        LOGGER.error("executor shutdown timed out");
      }
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
    _gossipServiceRunning.set(false);
  }
}
