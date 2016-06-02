package org.apache.gossip.model;

public class GossipMember {

  private String cluster;
  private String uri;
  private String id;
  private Long heartbeat;
  
  public GossipMember(){
    
  }
  
  public GossipMember(String cluster, String uri, String id, Long heartbeat){
    this.cluster = cluster;
    this.uri = uri;
    this.id = id;
    this.heartbeat = heartbeat;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Long getHeartbeat() {
    return heartbeat;
  }

  public void setHeartbeat(Long heartbeat) {
    this.heartbeat = heartbeat;
  }
  
}
