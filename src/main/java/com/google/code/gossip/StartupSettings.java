package com.google.code.gossip;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This object represents the settings used when starting the gossip service.
 *
 * @author harmenw
 */
public class StartupSettings {

  /** The port to start the gossip service on. */
  private int _port;

  /** The logging level of the gossip service. */
  private int _logLevel;

  /** The gossip settings used at startup. */
  private final GossipSettings _gossipSettings;

  /** The list with gossip members to start with. */
  private final List<GossipMember> _gossipMembers;

  /**
   * Constructor.
   *
   * @param port
   *          The port to start the service on.
   */
  public StartupSettings(int port, int logLevel) {
    this(port, logLevel, new GossipSettings());
  }

  /**
   * Constructor.
   *
   * @param port
   *          The port to start the service on.
   */
  public StartupSettings(int port, int logLevel, GossipSettings gossipSettings) {
    _port = port;
    _logLevel = logLevel;
    _gossipSettings = gossipSettings;
    _gossipMembers = new ArrayList<>();
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
   * Set the log level of the gossip service.
   *
   * @param logLevel
   *          The log level({LogLevel}).
   */
  public void setLogLevel(int logLevel) {
    _logLevel = logLevel;
  }

  /**
   * Get the log level of the gossip service.
   *
   * @return The log level.
   */
  public int getLogLevel() {
    return _logLevel;
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

    // Get the log level from the config file.
    int logLevel = LogLevel.fromString(jsonObject.getString("log_level"));

    // Get the gossip_interval from the config file.
    int gossipInterval = jsonObject.getInt("gossip_interval");

    // Get the cleanup_interval from the config file.
    int cleanupInterval = jsonObject.getInt("cleanup_interval");

    System.out.println("Config [port: " + port + ", log_level: " + logLevel + ", gossip_interval: "
            + gossipInterval + ", cleanup_interval: " + cleanupInterval + "]");

    // Initiate the settings with the port number.
    StartupSettings settings = new StartupSettings(port, logLevel, new GossipSettings(
            gossipInterval, cleanupInterval));

    // Now iterate over the members from the config file and add them to the settings.
    System.out.print("Config-members [");
    JSONArray membersJSON = jsonObject.getJSONArray("members");
    for (int i = 0; i < membersJSON.length(); i++) {
      JSONObject memberJSON = membersJSON.getJSONObject(i);
      RemoteGossipMember member = new RemoteGossipMember(memberJSON.getString("host"),
              memberJSON.getInt("port"), "");
      settings.addGossipMember(member);
      System.out.print(member.getAddress());
      if (i < (membersJSON.length() - 1))
        System.out.print(", ");
    }
    System.out.println("]");

    // Return the created settings object.
    return settings;
  }
}
