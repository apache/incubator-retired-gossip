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
package org.apache.gossip.manager;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.gossip.GossipMember;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalGossipMember;
import org.apache.gossip.crdt.Crdt;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.manager.handlers.MessageInvoker;
import org.apache.gossip.manager.impl.OnlyProcessReceivedPassiveGossipThread;
import org.apache.gossip.model.GossipDataMessage;
import org.apache.gossip.model.SharedGossipDataMessage;
import org.apache.gossip.model.ShutdownMessage;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public abstract class GossipManager {

  public static final Logger LOGGER = Logger.getLogger(GossipManager.class);

  private final ConcurrentSkipListMap<LocalGossipMember, GossipState> members;
  private final LocalGossipMember me;
  private final GossipSettings settings;
  private final AtomicBoolean gossipServiceRunning;
  private final GossipListener listener;
  private AbstractActiveGossiper activeGossipThread;
  private PassiveGossipThread passiveGossipThread;
  private ExecutorService gossipThreadExecutor;
  private final GossipCore gossipCore;
  private final DataReaper dataReaper;
  private final Clock clock;
  private final ScheduledExecutorService scheduledServiced;
  private final MetricRegistry registry;
  private final RingStatePersister ringState;
  private final UserDataPersister userDataState;
  private final ObjectMapper objectMapper;

  private final MessageInvoker messageInvoker;

  public GossipManager(String cluster,
                       URI uri, String id, Map<String, String> properties, GossipSettings settings,
                       List<GossipMember> gossipMembers, GossipListener listener, MetricRegistry registry,
                       ObjectMapper objectMapper, MessageInvoker messageInvoker) {
    this.settings = settings;
    this.messageInvoker = messageInvoker;
    clock = new SystemClock();    
    me = new LocalGossipMember(cluster, uri, id, clock.nanoTime(), properties,
            settings.getWindowSize(), settings.getMinimumSamples(), settings.getDistribution());
    gossipCore = new GossipCore(this, registry);
    dataReaper = new DataReaper(gossipCore, clock);
    members = new ConcurrentSkipListMap<>();
    for (GossipMember startupMember : gossipMembers) {
      if (!startupMember.equals(me)) {
        LocalGossipMember member = new LocalGossipMember(startupMember.getClusterName(),
                startupMember.getUri(), startupMember.getId(),
                clock.nanoTime(), startupMember.getProperties(), settings.getWindowSize(), 
                settings.getMinimumSamples(), settings.getDistribution());
        //TODO should members start in down state?
        members.put(member, GossipState.DOWN);
      }
    }
    gossipThreadExecutor = Executors.newCachedThreadPool();
    gossipServiceRunning = new AtomicBoolean(true);
    this.listener = listener;
    this.scheduledServiced = Executors.newScheduledThreadPool(1);
    this.registry = registry;
    this.ringState = new RingStatePersister(this);
    this.userDataState = new UserDataPersister(this, this.gossipCore);
    this.objectMapper = objectMapper;
    readSavedRingState();
    readSavedDataState();
  }

  public MessageInvoker getMessageInvoker() {
    return messageInvoker;
  }

  public ConcurrentSkipListMap<LocalGossipMember, GossipState> getMembers() {
    return members;
  }

  public GossipSettings getSettings() {
    return settings;
  }

  /**
   * @return a read only list of members found in the DOWN state.
   */
  public List<LocalGossipMember> getDeadMembers() {
    return Collections.unmodifiableList(
            members.entrySet()
            .stream()
            .filter(entry -> GossipState.DOWN.equals(entry.getValue()))
            .map(Entry::getKey).collect(Collectors.toList()));
  }

  /**
   * 
   * @return a read only list of members found in the UP state
   */
  public List<LocalGossipMember> getLiveMembers() {
    return Collections.unmodifiableList(
            members.entrySet()
            .stream()
            .filter(entry -> GossipState.UP.equals(entry.getValue()))
            .map(Entry::getKey).collect(Collectors.toList()));
  }

  public LocalGossipMember getMyself() {
    return me;
  }

  private AbstractActiveGossiper constructActiveGossiper(){
    try {
      Constructor<?> c = Class.forName(settings.getActiveGossipClass()).getConstructor(GossipManager.class, GossipCore.class, MetricRegistry.class);
      return (AbstractActiveGossiper) c.newInstance(this, gossipCore, registry);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Starts the client. Specifically, start the various cycles for this protocol. Start the gossip
   * thread and start the receiver thread.
   */
  public void init() {
    passiveGossipThread = new OnlyProcessReceivedPassiveGossipThread(this, gossipCore);
    gossipThreadExecutor.execute(passiveGossipThread);
    activeGossipThread = constructActiveGossiper();
    activeGossipThread.init();
    dataReaper.init();
    scheduledServiced.scheduleAtFixedRate(ringState, 60, 60, TimeUnit.SECONDS);
    scheduledServiced.scheduleAtFixedRate(userDataState, 60, 60, TimeUnit.SECONDS);
    scheduledServiced.scheduleAtFixedRate(() -> {
      try {
        for (Entry<LocalGossipMember, GossipState> entry : members.entrySet()) {
          boolean userDown = processOptomisticShutdown(entry);
          if (userDown)
            continue;
          Double result = null;
          try {
            result = entry.getKey().detect(clock.nanoTime());
            if (result != null) {
              if (result > settings.getConvictThreshold() && entry.getValue() == GossipState.UP) {
                members.put(entry.getKey(), GossipState.DOWN);
                listener.gossipEvent(entry.getKey(), GossipState.DOWN);
              }
              if (result <= settings.getConvictThreshold() && entry.getValue() == GossipState.DOWN) {
                members.put(entry.getKey(), GossipState.UP);
                listener.gossipEvent(entry.getKey(), GossipState.UP);
              }
            }
          } catch (IllegalArgumentException ex) {
            //0.0 returns throws exception computing the mean. 
            long now = clock.nanoTime(); 
            long nowInMillis = TimeUnit.MILLISECONDS.convert(now,TimeUnit.NANOSECONDS);
            if (nowInMillis - settings.getCleanupInterval() > entry.getKey().getHeartbeat() && entry.getValue() == GossipState.UP){
              LOGGER.warn("Marking down");
              members.put(entry.getKey(), GossipState.DOWN);
              listener.gossipEvent(entry.getKey(), GossipState.DOWN);
            }
          } //end catch
        } // end for
      } catch (RuntimeException ex) {
        LOGGER.warn("scheduled state had exception", ex);
      }
    }, 0, 100, TimeUnit.MILLISECONDS);
    LOGGER.debug("The GossipManager is started.");
  }

  /**
   * If we have a special key the per-node data that means that the node has sent us 
   * a pre-emptive shutdown message. We process this so node is seen down sooner
   * @param l member to consider
   * @return true if node forced down
   */
  public boolean processOptomisticShutdown(Entry<LocalGossipMember, GossipState> l){
    GossipDataMessage m = findPerNodeGossipData(l.getKey().getId(), ShutdownMessage.PER_NODE_KEY);
    if (m == null){
      return false;
    }
    ShutdownMessage s = (ShutdownMessage) m.getPayload();
    if (s.getShutdownAtNanos() > l.getKey().getHeartbeat()){
      if (l.getValue() == GossipState.UP){
        members.put(l.getKey(), GossipState.DOWN);
        listener.gossipEvent(l.getKey(), GossipState.DOWN);
      } else {
        members.put(l.getKey(), GossipState.DOWN);
      }
      return true;
    }
    return false;
  }
  
  private void readSavedRingState() {
    for (LocalGossipMember l : ringState.readFromDisk()){
      LocalGossipMember member = new LocalGossipMember(l.getClusterName(),
              l.getUri(), l.getId(),
              clock.nanoTime(), l.getProperties(), settings.getWindowSize(), 
              settings.getMinimumSamples(), settings.getDistribution());
      members.putIfAbsent(member, GossipState.DOWN);
    }
  }
  
  private void readSavedDataState() {
    for (Entry<String, ConcurrentHashMap<String, GossipDataMessage>> l : userDataState.readPerNodeFromDisk().entrySet()){
      for (Entry<String, GossipDataMessage> j : l.getValue().entrySet()){
        gossipCore.addPerNodeData(j.getValue());
      }
    }
    for (Entry<String, SharedGossipDataMessage> l: userDataState.readSharedDataFromDisk().entrySet()){
      gossipCore.addSharedData(l.getValue());
    }
  }

  /**
   * Shutdown the gossip service.
   */
  public void shutdown() {
    gossipServiceRunning.set(false);
    gossipThreadExecutor.shutdown();
    gossipCore.shutdown();
    dataReaper.close();
    if (passiveGossipThread != null) {
      passiveGossipThread.shutdown();
    }
    if (activeGossipThread != null) {
      activeGossipThread.shutdown();
    }
    try {
      boolean result = gossipThreadExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);
      if (!result) {
        LOGGER.error("executor shutdown timed out");
      }
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
    gossipThreadExecutor.shutdownNow();
    scheduledServiced.shutdown();
    try {
      scheduledServiced.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
    scheduledServiced.shutdownNow();
  }
  
  public void gossipPerNodeData(GossipDataMessage message){
    Objects.nonNull(message.getKey());
    Objects.nonNull(message.getTimestamp());
    Objects.nonNull(message.getPayload());
    message.setNodeId(me.getId());
    gossipCore.addPerNodeData(message);
  }
  
  public void gossipSharedData(SharedGossipDataMessage message){
    Objects.nonNull(message.getKey());
    Objects.nonNull(message.getTimestamp());
    Objects.nonNull(message.getPayload());
    message.setNodeId(me.getId());
    gossipCore.addSharedData(message);
  }
  

  @SuppressWarnings("rawtypes")
  public Crdt findCrdt(String key){
    SharedGossipDataMessage l = gossipCore.getSharedData().get(key);
    if (l == null){
      return null;
    }
    if (l.getExpireAt() < clock.currentTimeMillis()){
      return null;
    } else {
      return (Crdt) l.getPayload();
    }
  }
  
  @SuppressWarnings("rawtypes")
  public Crdt merge(SharedGossipDataMessage message){
    Objects.nonNull(message.getKey());
    Objects.nonNull(message.getTimestamp());
    Objects.nonNull(message.getPayload());
    message.setNodeId(me.getId());
    if (! (message.getPayload() instanceof Crdt)){
      throw new IllegalArgumentException("Not a subclass of CRDT " + message.getPayload());
    }
    return gossipCore.merge(message);
  }
  
  public GossipDataMessage findPerNodeGossipData(String nodeId, String key){
    ConcurrentHashMap<String, GossipDataMessage> j = gossipCore.getPerNodeData().get(nodeId);
    if (j == null){
      return null;
    } else {
      GossipDataMessage l = j.get(key);
      if (l == null){
        return null;
      }
      if (l.getExpireAt() != null && l.getExpireAt() < clock.currentTimeMillis()) {
        return null;
      }
      return l;
    }
  }
  
  public SharedGossipDataMessage findSharedGossipData(String key){
    SharedGossipDataMessage l = gossipCore.getSharedData().get(key);
    if (l == null){
      return null;
    }
    if (l.getExpireAt() < clock.currentTimeMillis()){
      return null;
    } else {
      return l;
    }
  }

  public DataReaper getDataReaper() {
    return dataReaper;
  }

  public RingStatePersister getRingState() {
    return ringState;
  }
            
  public UserDataPersister getUserDataState() {
    return userDataState;
  }

  public Clock getClock() {
    return clock;
  }

  public ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  public MetricRegistry getRegistry() {
    return registry;
  }
  
}
