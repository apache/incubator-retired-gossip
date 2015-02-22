package com.google.code.gossip.examples;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LogLevel;
import com.google.code.gossip.RemoteGossipMember;

/**
 * This class is an example of how one could use the gossip service. Here we start multiple gossip
 * clients on this host as specified in the config file.
 *
 * @author harmenw
 */
public class GossipExample extends Thread {
  /** The number of clients to start. */
  private static final int NUMBER_OF_CLIENTS = 4;

  /**
   * @param args
   */
  public static void main(String[] args) {
    new GossipExample();
  }

  /**
   * Constructor. This will start the this thread.
   */
  public GossipExample() {
    start();
  }

  /**
   * @see java.lang.Thread#run()
   */
  public void run() {
    try {
      GossipSettings settings = new GossipSettings();

      List<GossipService> clients = new ArrayList<>();

      // Get my ip address.
      String myIpAddress = InetAddress.getLocalHost().getHostAddress();

      // Create the gossip members and put them in a list and give them a port number starting with
      // 2000.
      List<GossipMember> startupMembers = new ArrayList<>();
      for (int i = 0; i < NUMBER_OF_CLIENTS; ++i) {
        startupMembers.add(new RemoteGossipMember(myIpAddress, 2000 + i, ""));
      }

      // Lets start the gossip clients.
      // Start the clients, waiting cleaning-interval + 1 second between them which will show the
      // dead list handling.
      for (GossipMember member : startupMembers) {
        GossipService gossipService = new GossipService(myIpAddress, member.getPort(), "",
                LogLevel.DEBUG, startupMembers, settings, null);
        clients.add(gossipService);
        gossipService.start();
        sleep(settings.getCleanupInterval() + 1000);
      }

      // After starting all gossip clients, first wait 10 seconds and then shut them down.
      sleep(10000);
      System.err.println("Going to shutdown all services...");
      // Since they all run in the same virtual machine and share the same executor, if one is
      // shutdown they will all stop.
      clients.get(0).shutdown();

    } catch (UnknownHostException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
