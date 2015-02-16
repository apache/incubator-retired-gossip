
Gossip protocol is a method for a group of nodes to discover and check the livelyness of a cluster. More information can be found at http://en.wikipedia.org/wiki/Gossip_protocol.

The original implementation was forked from https://code.google.com/p/java-gossip/. Several bug fixes and changes have already been added.

Usage
-----

To gossip you need one or more seed nodes. Seed is just a list of places to initially connect to. 

    GossipSettings settings = new GossipSettings();
    int seedNodes = 3;
    ArrayList<GossipMember> startupMembers = new ArrayList<GossipMember>();
    for (int i = 1; i < seedNodes+1; ++i) {
      startupMembers.add(new RemoteGossipMember("127.0.0." + i, 2000, i + ""));
    }

Here we start five gossip processes and check that they discover each other. (Normally these are on different hosts but here we give each process a distinct local ip.

    ArrayList<GossipService> clients = new ArrayList<GossipService>();
    int clusterMembers = 5;
    for (int i = 1; i < clusterMembers+1; ++i) {
      GossipService gossipService = new GossipService("127.0.0." + i, 2000, i + "", 
        LogLevel.DEBUG, startupMembers, settings, null);
      clients.add(gossipService);
      gossipService.start();
    }

Later we can check that the nodes discover each other

    Thread.sleep(10000);
    for (int i = 0; i < clusterMembers; ++i) {
      Assert.assertEquals(4, clients.get(i).get_gossipManager().getMemberList().size());
    }

Event Listener
------

The status can be polled using the getters that return immutable lists.

     List<LocalGossipMember> getMemberList()
     public List<LocalGossipMember> getDeadList()

Users can also attach an event listener:

      GossipService gossipService = new GossipService("127.0.0." + i, 2000, i + "", LogLevel.DEBUG,
              startupMembers, settings,
              new GossipListener(){
        @Override
        public void gossipEvent(GossipMember member, GossipState state) {
          System.out.println(member+" "+ state);
        }
      });


Maven
------


You can get this software from maven central.

    <dependency>  
         <groupId>io.teknek</groupId>
        <artifactId>gossip</artifactId>
        <version>${pick_the_latest_version}</version>
    </dependency>
