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
  private String _id;

  /** The port to start the gossip service on. */
  private int _port;

  /** The gossip settings used at startup. */
  private final GossipSettings _gossipSettings;

  /** The list with gossip members to start with. */
  private final List<GossipMember> _gossipMembers;

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
  public StartupSettings(String id, int port, int logLevel) {
    this(id, port, new GossipSettings());
  }

  /**
   * Constructor.
   *
   * @param id
   *          The id to be used for this service
   * @param port
   *          The port to start the service on.
   */
  public StartupSettings(String id, int port, GossipSettings gossipSettings) {
    _id = id;
    _port = port;
    _gossipSettings = gossipSettings;
    _gossipMembers = new ArrayList<>();
  }

  /**
   * Set the id to be used for this service.
   *
   * @param id
   *          The id for this service.
   */
  public void setId( String id ) {
    _id = id;
  }

  /**
   * Get the id for this service.
   *
   * @return the service's id.
   */
  public String getId() {
    return _id;
  }

  /**
   * Set the port of the gossip service.
   *
   * @param port
   *          The port for the gossip service.
   */
  public void setPort(int port) {
    _port = port;
  }

  /**
   * Get the port for the gossip service.
   *
   * @return The port of the gossip service.
   */
  public int getPort() {
    return _port;
  }

  /**
   * Get the GossipSettings.
   *
   * @return The GossipSettings object.
   */
  public GossipSettings getGossipSettings() {
    return _gossipSettings;
  }

  /**
   * Add a gossip member to the list of members to start with.
   *
   * @param member
   *          The member to add.
   */
  public void addGossipMember(GossipMember member) {
    _gossipMembers.add(member);
  }

  /**
   * Get the list with gossip members.
   *
   * @return The gossip members.
   */
  public List<GossipMember> getGossipMembers() {
    return _gossipMembers;
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
    BufferedReader br = new BufferedReader(new FileReader(jsonFile));
    StringBuffer buffer = new StringBuffer();
    String line;
    while ((line = br.readLine()) != null) {
      buffer.append(line.trim());
    }

    // Lets parse the String as JSON.
    JSONObject jsonObject = new JSONArray(buffer.toString()).getJSONObject(0);

    // Now get the port number.
    int port = jsonObject.getInt("port");

    // Get the id to be used
    String id = jsonObject.getString("id");

    // Get the gossip_interval from the config file.
    int gossipInterval = jsonObject.getInt("gossip_interval");

    // Get the cleanup_interval from the config file.
    int cleanupInterval = jsonObject.getInt("cleanup_interval");

    // Initiate the settings with the port number.
    StartupSettings settings = new StartupSettings(id, port, new GossipSettings(
            gossipInterval, cleanupInterval));

    // Now iterate over the members from the config file and add them to the settings.
    String configMembersDetails = "Config-members [";
    JSONArray membersJSON = jsonObject.getJSONArray("members");
    for (int i = 0; i < membersJSON.length(); i++) {
      JSONObject memberJSON = membersJSON.getJSONObject(i);
      RemoteGossipMember member = new RemoteGossipMember(memberJSON.getString("host"),
              memberJSON.getInt("port"), "");
      settings.addGossipMember(member);
      configMembersDetails += member.getAddress();
      if (i < (membersJSON.length() - 1))
        configMembersDetails += ", ";
    }
    log.info( configMembersDetails + "]" );

    // Return the created settings object.
    return settings;
  }
}
