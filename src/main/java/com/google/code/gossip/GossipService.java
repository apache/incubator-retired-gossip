package com.google.code.gossip;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;

import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.random.RandomGossipManager;

/**
 * This object represents the service which is responsible for gossiping with other gossip members.
 * 
 * @author joshclemm, harmenw
 */
public class GossipService {
	
	/** A instance variable holding the log level. */
	private int _logLevel = LogLevel.INFO;
	
	private GossipManager _gossipManager;
	
	/**
	 * Constructor with the default settings.
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public GossipService(StartupSettings startupSettings) throws InterruptedException, UnknownHostException {
		this(InetAddress.getLocalHost().getHostAddress(), startupSettings.getPort(), startupSettings.getLogLevel(), startupSettings.getGossipMembers(), startupSettings.getGossipSettings());
	}

	/**
	 * Setup the client's lists, gossiping parameters, and parse the startup config file.
	 * @throws SocketException
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public GossipService(String ipAddress, int port, int logLevel, ArrayList<GossipMember> gossipMembers, GossipSettings settings) throws InterruptedException, UnknownHostException {
		// Set the logging level.
		_logLevel = logLevel;
		
		_gossipManager = new RandomGossipManager(ipAddress, port, settings, gossipMembers);
	}
	
	public void start() {
		_gossipManager.start();
	}
	
	public void shutdown() {
		_gossipManager.shutdown();
	}
	
	public static void error(Object message) {
		//if (_logLevel >= LogLevel.ERROR) printMessage(message, System.err);
		printMessage(message, System.err);
	}
	
	public static void info(Object message) {
		//if (_logLevel >= LogLevel.INFO) printMessage(message, System.out);
		printMessage(message, System.out);
	}
	
	public static void debug(Object message) {
		//if (_logLevel >= LogLevel.DEBUG) printMessage(message, System.out);
		printMessage(message, System.out);
	}
	
	private static void printMessage(Object message, PrintStream out) {
		/**String addressString = "unknown";
		if (_me != null)
			addressString = _me.getAddress();
		out.println("[" + addressString + "][" + new Date().toString() + "] " + message);*/
		out.println("[" + new Date().toString() + "] " + message);
	}
}
