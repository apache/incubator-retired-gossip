package com.google.code.gossip.model;

public class GossipMember {

  private String cluster;
  private String host;
  private Integer port;
  private String id;
  private Long heartbeat;
  
  public GossipMember(){
    
  }
  
  public GossipMember(String cluster, String host, Integer port, String id, Long heartbeat){
    this.cluster=cluster;
    this.host= host;
    this.port = port;
    this.id = id;
    
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
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
