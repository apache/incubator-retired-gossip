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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.gossip.GossipMember;
import org.apache.gossip.LocalGossipMember;
import org.apache.gossip.RemoteGossipMember;
import org.apache.gossip.crdt.Crdt;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.model.*;
import org.apache.gossip.udp.Trackable;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class GossipCore implements GossipCoreConstants {

  public static final Logger LOGGER = Logger.getLogger(GossipCore.class);
  private final GossipManager gossipManager;
  private ConcurrentHashMap<String, Base> requests;
  private ThreadPoolExecutor service;
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>> perNodeData;
  private final ConcurrentHashMap<String, SharedGossipDataMessage> sharedData;
  private final BlockingQueue<Runnable> workQueue;
  private final PKCS8EncodedKeySpec privKeySpec;
  private final PrivateKey privKey;
  private final Meter messageSerdeException;
  private final Meter tranmissionException;
  private final Meter tranmissionSuccess;

  public GossipCore(GossipManager manager, MetricRegistry metrics){
    this.gossipManager = manager;
    requests = new ConcurrentHashMap<>();
    workQueue = new ArrayBlockingQueue<>(1024);
    service = new ThreadPoolExecutor(1, 5, 1, TimeUnit.SECONDS, workQueue, new ThreadPoolExecutor.DiscardOldestPolicy());
    perNodeData = new ConcurrentHashMap<>();
    sharedData = new ConcurrentHashMap<>();
    metrics.register(WORKQUEUE_SIZE, (Gauge<Integer>)() -> workQueue.size());
    metrics.register(PER_NODE_DATA_SIZE, (Gauge<Integer>)() -> perNodeData.size());
    metrics.register(SHARED_DATA_SIZE, (Gauge<Integer>)() ->  sharedData.size());
    metrics.register(REQUEST_SIZE, (Gauge<Integer>)() ->  requests.size());
    metrics.register(THREADPOOL_ACTIVE, (Gauge<Integer>)() ->  service.getActiveCount());
    metrics.register(THREADPOOL_SIZE, (Gauge<Integer>)() ->  service.getPoolSize());
    messageSerdeException = metrics.meter(MESSAGE_SERDE_EXCEPTION);
    tranmissionException = metrics.meter(MESSAGE_TRANSMISSION_EXCEPTION);
    tranmissionSuccess = metrics.meter(MESSAGE_TRANSMISSION_SUCCESS);

    if (manager.getSettings().isSignMessages()){
      File privateKey = new File(manager.getSettings().getPathToKeyStore(), manager.getMyself().getId());
      File publicKey = new File(manager.getSettings().getPathToKeyStore(), manager.getMyself().getId() + ".pub");
      if (!privateKey.exists()){
        throw new IllegalArgumentException("private key not found " + privateKey);
      }
      if (!publicKey.exists()){
        throw new IllegalArgumentException("public key not found " + publicKey);
      }
      try (FileInputStream keyfis = new FileInputStream(privateKey)) {
        byte[] encKey = new byte[keyfis.available()];
        keyfis.read(encKey);
        keyfis.close();
        privKeySpec = new PKCS8EncodedKeySpec(encKey);
        KeyFactory keyFactory = KeyFactory.getInstance("DSA");
        privKey = keyFactory.generatePrivate(privKeySpec);
      } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
        throw new RuntimeException("failed hard", e);
      }
    } else {
      privKeySpec = null;
      privKey = null;
    }
  }

  private byte [] sign(byte [] bytes){
    Signature dsa;
    try {
      dsa = Signature.getInstance("SHA1withDSA", "SUN");
      dsa.initSign(privKey);
      dsa.update(bytes);
      return dsa.sign();
    } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException | SignatureException e) {
      throw new RuntimeException(e);
    } 
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void addSharedData(SharedGossipDataMessage message) {
    SharedGossipDataMessage previous = sharedData.get(message.getKey());
    if (previous == null) {
      sharedData.putIfAbsent(message.getKey(), message);
    } else {
      if (message.getPayload() instanceof Crdt){
        SharedGossipDataMessage m = sharedData.get(message.getKey());
        SharedGossipDataMessage merged = new SharedGossipDataMessage();
        merged.setExpireAt(message.getExpireAt());
        merged.setKey(m.getKey());
        merged.setNodeId(message.getNodeId());
        merged.setTimestamp(message.getTimestamp());
        merged.setPayload( ((Crdt) message.getPayload()).merge((Crdt)m.getPayload()));
        sharedData.put(m.getKey(), merged);
      } else {
        if (previous.getTimestamp() < message.getTimestamp()) {
          sharedData.replace(message.getKey(), previous, message);
        }
      }
    }
  }

  public void addPerNodeData(GossipDataMessage message){
    ConcurrentHashMap<String,GossipDataMessage> nodeMap = new ConcurrentHashMap<>();
    nodeMap.put(message.getKey(), message);
    nodeMap = perNodeData.putIfAbsent(message.getNodeId(), nodeMap);
    if (nodeMap != null){
      GossipDataMessage current = nodeMap.get(message.getKey());
      if (current == null){
        nodeMap.putIfAbsent(message.getKey(), message);
      } else {
        if (current.getTimestamp() < message.getTimestamp()){
          nodeMap.replace(message.getKey(), current, message);
        }
      }
    }
  }

  public ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>> getPerNodeData(){
    return perNodeData;
  }

  public ConcurrentHashMap<String, SharedGossipDataMessage> getSharedData() {
    return sharedData;
  }

  public void shutdown(){
    service.shutdown();
    try {
      service.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn(e);
    }
    service.shutdownNow();
  }

  public void receive(Base base) {
    if (!gossipManager.getMessageInvoker().invoke(this, gossipManager, base)) {
      LOGGER.warn("received message can not be handled");
    }
  }

  /**
   * Sends a blocking message.
   * @param message
   * @param uri
   * @throws RuntimeException if data can not be serialized or in transmission error
   */
  private void sendInternal(Base message, URI uri){
    byte[] json_bytes;
    try {
      if (privKey == null){
        json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(message);
      } else {
        SignedPayload p = new SignedPayload();
        p.setData(gossipManager.getObjectMapper().writeValueAsString(message).getBytes());
        p.setSignature(sign(p.getData()));
        json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(p);
      }
    } catch (IOException e) {
      messageSerdeException.mark();
      throw new RuntimeException(e);
    }
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setSoTimeout(gossipManager.getSettings().getGossipInterval() * 2);
      InetAddress dest = InetAddress.getByName(uri.getHost());
      DatagramPacket datagramPacket = new DatagramPacket(json_bytes, json_bytes.length, dest, uri.getPort());
      socket.send(datagramPacket);
      tranmissionSuccess.mark();
    } catch (IOException e) {
      tranmissionException.mark();
      throw new RuntimeException(e);
    }
  }

  public Response send(Base message, URI uri){
    if (LOGGER.isDebugEnabled()){
      LOGGER.debug("Sending " + message);
      LOGGER.debug("Current request queue " + requests);
    }

    final Trackable t;
    if (message instanceof Trackable){
      t = (Trackable) message;
    } else {
      t = null;
    }
    sendInternal(message, uri);
    if (t == null){
      return null;
    }
    final Future<Response> response = service.submit( new Callable<Response>(){
      @Override
      public Response call() throws Exception {
        while(true){
          Base b = requests.remove(t.getUuid() + "/" + t.getUriFrom());
          if (b != null){
            return (Response) b;
          }
          try {
            Thread.sleep(0, 555555);
          } catch (InterruptedException e) {

          }
        }
      }
    });

    try {
      //TODO this needs to be a setting base on attempts/second
      return response.get(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOGGER.debug(e.getMessage(), e);
      return null;
    } catch (TimeoutException e) {
      boolean cancelled = response.cancel(true);
      LOGGER.debug(String.format("Threadpool timeout attempting to contact %s, cancelled ? %b", uri.toString(), cancelled));
      return null;
    } finally {
      if (t != null){
        requests.remove(t.getUuid() + "/" + t.getUriFrom());
      }
    }
  }

  /**
   * Sends a message across the network while blocking. Catches and ignores IOException in transmission. Used
   * when the protocol for the message is not to wait for a response
   * @param message the message to send
   * @param u the uri to send it to
   */
  public void sendOneWay(Base message, URI u){
    byte[] json_bytes;
    try {
      if (privKey == null){
        json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(message);
      } else {
        SignedPayload p = new SignedPayload();
        p.setData(gossipManager.getObjectMapper().writeValueAsString(message).getBytes());
        p.setSignature(sign(p.getData()));
        json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(p);
      }
    } catch (IOException e) {
      messageSerdeException.mark();
      throw new RuntimeException(e);
    }
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setSoTimeout(gossipManager.getSettings().getGossipInterval() * 2);
      InetAddress dest = InetAddress.getByName(u.getHost());
      DatagramPacket datagramPacket = new DatagramPacket(json_bytes, json_bytes.length, dest, u.getPort());
      socket.send(datagramPacket);
      tranmissionSuccess.mark();
    } catch (IOException ex) {
      tranmissionException.mark();
      LOGGER.debug("Send one way failed", ex);
    }
  }

  public void addRequest(String k, Base v) {
    requests.put(k, v);
  }

  /**
   * Merge lists from remote members and update heartbeats
   *
   * @param gossipManager
   * @param senderMember
   * @param remoteList
   *
   */
  public void mergeLists(GossipManager gossipManager, RemoteGossipMember senderMember,
          List<GossipMember> remoteList) {
    if (LOGGER.isDebugEnabled()){
      debugState(senderMember, remoteList);
    }
    for (LocalGossipMember i : gossipManager.getDeadMembers()) {
      if (i.getId().equals(senderMember.getId())) {
        LOGGER.debug(gossipManager.getMyself() + " contacted by dead member " + senderMember.getUri());
        i.recordHeartbeat(senderMember.getHeartbeat());
        i.setHeartbeat(senderMember.getHeartbeat());
        //TODO consider forcing an UP here
      }
    }
    for (GossipMember remoteMember : remoteList) {
      if (remoteMember.getId().equals(gossipManager.getMyself().getId())) {
        continue;
      }
      LocalGossipMember aNewMember = new LocalGossipMember(remoteMember.getClusterName(),
      remoteMember.getUri(),
      remoteMember.getId(),
      remoteMember.getHeartbeat(),
      remoteMember.getProperties(),
      gossipManager.getSettings().getWindowSize(),
      gossipManager.getSettings().getMinimumSamples(),
      gossipManager.getSettings().getDistribution());
      aNewMember.recordHeartbeat(remoteMember.getHeartbeat());
      Object result = gossipManager.getMembers().putIfAbsent(aNewMember, GossipState.UP);
      if (result != null){
        for (Entry<LocalGossipMember, GossipState> localMember : gossipManager.getMembers().entrySet()){
          if (localMember.getKey().getId().equals(remoteMember.getId())){
            localMember.getKey().recordHeartbeat(remoteMember.getHeartbeat());
            localMember.getKey().setHeartbeat(remoteMember.getHeartbeat());
            localMember.getKey().setProperties(remoteMember.getProperties());
          }
        }
      }
    }
    if (LOGGER.isDebugEnabled()){
      debugState(senderMember, remoteList);
    }
  }

  private void debugState(RemoteGossipMember senderMember,
          List<GossipMember> remoteList){
    LOGGER.warn(
          "-----------------------\n" +
          "Me " + gossipManager.getMyself() + "\n" +
          "Sender " + senderMember + "\n" +
          "RemoteList " + remoteList + "\n" +
          "Live " + gossipManager.getLiveMembers()+ "\n" +
          "Dead " + gossipManager.getDeadMembers()+ "\n" +
          "=======================");
  }

  @SuppressWarnings("rawtypes")
  public Crdt merge(SharedGossipDataMessage message) {
    for (;;){
      SharedGossipDataMessage ret = sharedData.putIfAbsent(message.getKey(), message);
      if (ret == null){
        return (Crdt) message.getPayload();
      }
      SharedGossipDataMessage copy = new SharedGossipDataMessage();
      copy.setExpireAt(message.getExpireAt());
      copy.setKey(message.getKey());
      copy.setNodeId(message.getNodeId());
      @SuppressWarnings("unchecked")
      Crdt merged = ((Crdt) ret.getPayload()).merge((Crdt) message.getPayload());
      message.setPayload(merged);
      boolean replaced = sharedData.replace(message.getKey(), ret, copy);
      if (replaced){
        return merged;
      }
    }
  }
}
