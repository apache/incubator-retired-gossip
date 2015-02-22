package com.google.code.gossip;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A abstract class representing a gossip member.
 *
 * @author joshclemm, harmenw
 */
public abstract class GossipMember implements Comparable<GossipMember>{

	public static final String JSON_HOST = "host";
	public static final String JSON_PORT = "port";
	public static final String JSON_HEARTBEAT = "heartbeat";
	public static final String JSON_ID = "id";
	protected final String _host;
	protected final int _port;
	protected int _heartbeat;
	/**
	 * The purpose of the id field is to be able for nodes to identify themselves beyond there host/port. For example
	 * an application might generate a persistent id so if they rejoin the cluster at a different host and port we 
	 * are aware it is the same node.
	 */
	protected String _id;

	/**
	 * Constructor.
	 * @param host The hostname or IP address.
	 * @param port The port number.
	 * @param heartbeat The current heartbeat.
	 * @param id an id that may be replaced after contact
	 */
	public GossipMember(String host, int port, String id, int heartbeat) {
		_host = host;
		_port = port;
		_id = id;
		_heartbeat = heartbeat;
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

	public int compareTo(GossipMember other){
	  return this.getAddress().compareTo(other.getAddress());
	}
}
