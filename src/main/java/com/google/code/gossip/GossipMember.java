package com.google.code.gossip;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A abstract class representing a gossip member.
 * 
 * @author joshclemm, harmenw
 */
public abstract class GossipMember {
	/** The JSON key for the host property. */
	public static final String JSON_HOST = "host";
	/** The JSON key for the port property. */
	public static final String JSON_PORT = "port";
	/** The JSON key for the heartbeat property. */
	public static final String JSON_HEARTBEAT = "heartbeat";
	
	public static final String JSON_ID = "id";

	/** The hostname or IP address of this gossip member. */
	protected String _host;
	
	/** The port number of this gossip member. */
	protected int _port;

	/** The current heartbeat of this gossip member. */
	protected int _heartbeat;
	
	protected String _id;
	
	/**
	 * Constructor.
	 * @param host The hostname or IP address.
	 * @param port The port number.
	 * @param heartbeat The current heartbeat.
	 */
	public GossipMember(String host, int port, String id, int heartbeat) {
		_host = host;
		_port = port;
		_heartbeat = heartbeat;
		_id = id;
	}
	
	/**
	 * Get the hostname or IP address of the remote gossip member.
	 * @return The hostname or IP address.
	 */
	public String getHost() {
		return _host;
	}
	
	/**
	 * Get the port number of the remote gossip member.
	 * @return The port number.
	 */
	public int getPort() {
		return _port;
	}

	/**
	 * The member address in the form IP/host:port
	 * Similar to the toString in {@link InetSocketAddress}
	 */
	public String getAddress() {
		return _host+":"+_port;
	}

	/**
	 * Get the heartbeat of this gossip member.
	 * @return The current heartbeat.
	 */
	public int getHeartbeat() {
		return _heartbeat;
	}
	
	/**
	 * Set the heartbeat of this gossip member.
	 * @param heartbeat The new heartbeat.
	 */
	public void setHeartbeat(int heartbeat) {
		this._heartbeat = heartbeat;
	}
	
	
	public String getId() {
    return _id;
  }

  public void setId(String _id) {
    this._id = _id;
  }

	public String toString() {
		return "Member [address=" + getAddress() + ", id=" + _id + ", heartbeat=" + _heartbeat + "]";
	}
	  
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		String address = getAddress();
		result = prime * result
		+ ((address == null) ? 0 : address.hashCode());
		return result;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			System.err.println("equals(): obj is null.");
			return false;
		}
		if (! (obj instanceof GossipMember) ) {
			System.err.println("equals(): obj is not of type GossipMember.");
			return false;
		}
		// The object is the same of they both have the same address (hostname and port).
		return getAddress().equals(((LocalGossipMember) obj).getAddress());
	}

	/**
	 * Get the JSONObject which is the JSON representation of this GossipMember.
	 * @return The JSONObject of this GossipMember.
	 */
	public JSONObject toJSONObject() {
		try {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put(JSON_HOST, _host);
			jsonObject.put(JSON_PORT, _port);
			jsonObject.put(JSON_ID, _id);
			jsonObject.put(JSON_HEARTBEAT, _heartbeat);
			return jsonObject;
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
}
