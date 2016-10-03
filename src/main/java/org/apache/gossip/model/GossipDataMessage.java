package org.apache.gossip.model;

public class GossipDataMessage extends Base {

  private String nodeId;
  private String key;
  private Object payload;
  private Long timestamp;
  private Long expireAt;
  
  public String getNodeId() {
    return nodeId;
  }
  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }
  public String getKey() {
    return key;
  }
  public void setKey(String key) {
    this.key = key;
  }
  public Object getPayload() {
    return payload;
  }
  public void setPayload(Object payload) {
    this.payload = payload;
  }
  public Long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }
  public Long getExpireAt() {
    return expireAt;
  }
  public void setExpireAt(Long expireAt) {
    this.expireAt = expireAt;
  }
  @Override
  public String toString() {
    return "GossipDataMessage [nodeId=" + nodeId + ", key=" + key + ", payload=" + payload
            + ", timestamp=" + timestamp + ", expireAt=" + expireAt + "]";
  }

  
  
}
