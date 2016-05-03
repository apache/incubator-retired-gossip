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
package com.google.code.gossip;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This object represents the settings used when starting the gossip service.
 * 
 * @author harmenw
 */
public class StartupSettings {
  private static final Logger log = Logger.getLogger(StartupSettings.class);

  /** The id to use fo the service */
  private String id;

  /** The port to start the gossip service on. */
  private int port;

  private String cluster;

  /** The gossip settings used at startup. */
  private final GossipSettings gossipSettings;

  /** The list with gossip members to start with. */
  private final List<GossipMember> gossipMembers;

  /**
   * Constructor.
   * 
   * @param id
   *          The id to be used for this service
   * @param port
   *          The port to start the service on.
   * @param logLevel
   *          unused
   */
  public StartupSettings(String id, int port, int logLevel, String cluster) {
    this(id, port, new GossipSettings(), cluster);
  }

  /**
   * Constructor.
   * 
   * @param id
   *          The id to be used for this service
   * @param port
   *          The port to start the service on.
   */
  public StartupSettings(String id, int port, GossipSettings gossipSettings, String cluster) {
    this.id = id;
    this.port = port;
    this.gossipSettings = gossipSettings;
    this.setCluster(cluster);
    gossipMembers = new ArrayList<>();
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getCluster() {
    return cluster;
  }

  /**
   * Set the id to be used for this service.
   * 
   * @param id
   *          The id for this service.
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Get the id for this service.
   * 
   * @return the service's id.
   */
  public String getId() {
    return id;
  }

  /**
   * Set the port of the gossip service.
   * 
   * @param port
   *          The port for the gossip service.
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Get the port for the gossip service.
   * 
   * @return The port of the gossip service.
   */
  public int getPort() {
    return port;
  }

  /**
   * Get the GossipSettings.
   * 
   * @return The GossipSettings object.
   */
  public GossipSettings getGossipSettings() {
    return gossipSettings;
  }

  /**
   * Add a gossip member to the list of members to start with.
   * 
   * @param member
   *          The member to add.
   */
  public void addGossipMember(GossipMember member) {
    gossipMembers.add(member);
  }

  /**
   * Get the list with gossip members.
   * 
   * @return The gossip members.
   */
  public List<GossipMember> getGossipMembers() {
    return gossipMembers;
  }

  /**
   * Parse the settings for the gossip service from a JSON file.
   * 
   * @param jsonFile
   *          The file object which refers to the JSON config file.
   * @return The StartupSettings object with the settings from the config file.
   * @throws JSONException
   *           Thrown when the file is not well-formed JSON.
   * @throws FileNotFoundException
   *           Thrown when the file cannot be found.
   * @throws IOException
   *           Thrown when reading the file gives problems.
   */
  public static StartupSettings fromJSONFile(File jsonFile) throws JSONException,
          FileNotFoundException, IOException {
    // Read the file to a String.
    StringBuffer buffer = new StringBuffer();
    try (BufferedReader br = new BufferedReader(new FileReader(jsonFile)) ){
      String line;
      while ((line = br.readLine()) != null) {
        buffer.append(line.trim());
      }
    }
    
    JSONObject jsonObject = new JSONArray(buffer.toString()).getJSONObject(0);
    int port = jsonObject.getInt("port");
    String id = jsonObject.getString("id");
    int gossipInterval = jsonObject.getInt("gossip_interval");
    int cleanupInterval = jsonObject.getInt("cleanup_interval");
    String cluster = jsonObject.getString("cluster");
    if (cluster == null){
      throw new IllegalArgumentException("cluster was null. It is required");
    }
    StartupSettings settings = new StartupSettings(id, port, new GossipSettings(gossipInterval,
            cleanupInterval), cluster);

    // Now iterate over the members from the config file and add them to the settings.
    String configMembersDetails = "Config-members [";
    JSONArray membersJSON = jsonObject.getJSONArray("members");
    for (int i = 0; i < membersJSON.length(); i++) {
      JSONObject memberJSON = membersJSON.getJSONObject(i);
      RemoteGossipMember member = new RemoteGossipMember(memberJSON.getString("cluster"),
              memberJSON.getString("host"), memberJSON.getInt("port"), "");
      settings.addGossipMember(member);
      configMembersDetails += member.getAddress();
      if (i < (membersJSON.length() - 1))
        configMembersDetails += ", ";
    }
    log.info(configMembersDetails + "]");

    // Return the created settings object.
    return settings;
  }
}
