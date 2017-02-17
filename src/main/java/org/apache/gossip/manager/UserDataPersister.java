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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.gossip.model.GossipDataMessage;
import org.apache.gossip.model.SharedGossipDataMessage;
import org.apache.log4j.Logger;

public class UserDataPersister implements Runnable {
  
  private static final Logger LOGGER = Logger.getLogger(UserDataPersister.class);
  private final GossipManager parent;
  private final GossipCore gossipCore; 
  
  UserDataPersister(GossipManager parent, GossipCore gossipCore){
    this.parent = parent;
    this.gossipCore = gossipCore;
  }
  
  File computeSharedTarget(){
    return new File(parent.getSettings().getPathToDataState(), "shareddata."
            + parent.getMyself().getClusterName() + "." + parent.getMyself().getId() + ".json");
  }
  
  File computePerNodeTarget() {
    return new File(parent.getSettings().getPathToDataState(), "pernodedata."
            + parent.getMyself().getClusterName() + "." + parent.getMyself().getId() + ".json");
  }
  
  @SuppressWarnings("unchecked")
  ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>> readPerNodeFromDisk(){
    if (!parent.getSettings().isPersistDataState()){
      return new ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>>();
    }
    try (FileInputStream fos = new FileInputStream(computePerNodeTarget())){
      return parent.getObjectMapper().readValue(fos, ConcurrentHashMap.class);
    } catch (IOException e) {
      LOGGER.debug(e);
    }
    return new ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>>();
  }
  
  void writePerNodeToDisk(){
    if (!parent.getSettings().isPersistDataState()){
      return;
    }
    try (FileOutputStream fos = new FileOutputStream(computePerNodeTarget())){
      parent.getObjectMapper().writeValue(fos, gossipCore.getPerNodeData());
    } catch (IOException e) {
      LOGGER.warn(e);
    }
  }
  
  void writeSharedToDisk(){
    if (!parent.getSettings().isPersistDataState()){
      return;
    }
    try (FileOutputStream fos = new FileOutputStream(computeSharedTarget())){
      parent.getObjectMapper().writeValue(fos, gossipCore.getSharedData());
    } catch (IOException e) {
      LOGGER.warn(e);
    }
  }

  @SuppressWarnings("unchecked")
  ConcurrentHashMap<String, SharedGossipDataMessage> readSharedDataFromDisk(){
    if (!parent.getSettings().isPersistRingState()){
      return new ConcurrentHashMap<String, SharedGossipDataMessage>();
    }
    try (FileInputStream fos = new FileInputStream(computeSharedTarget())){
      return parent.getObjectMapper().readValue(fos, ConcurrentHashMap.class);
    } catch (IOException e) {
      LOGGER.debug(e);
    }
    return new ConcurrentHashMap<String, SharedGossipDataMessage>();
  }
  
  /**
   * Writes all pernode and shared data to disk 
   */
  @Override
  public void run() {
    writePerNodeToDisk();
    writeSharedToDisk();
  }
}
