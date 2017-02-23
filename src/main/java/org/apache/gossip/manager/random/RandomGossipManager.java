/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.manager.random;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.gossip.GossipMember;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.crdt.CrdtModule;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.handlers.DefaultMessageInvoker;
import org.apache.gossip.manager.handlers.MessageInvoker;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RandomGossipManager extends GossipManager {

  public static ManagerBuilder newBuilder() {
    return new ManagerBuilder();
  }

  public static final class ManagerBuilder {
    private String cluster;
    private URI uri;
    private String id;
    private GossipSettings settings;
    private List<GossipMember> gossipMembers;
    private GossipListener listener;
    private MetricRegistry registry;
    private Map<String,String> properties;
    private ObjectMapper objectMapper;
    private MessageInvoker messageInvoker;

    private ManagerBuilder() {}

    private void checkArgument(boolean check, String msg) {
      if (!check) {
        throw new IllegalArgumentException(msg);
      }
    }

    public ManagerBuilder cluster(String cluster) {
      this.cluster = cluster;
      return this;
    }
    
    public ManagerBuilder properties(Map<String,String> properties) {
      this.properties = properties;
      return this;
    }

    public ManagerBuilder withId(String id) {
      this.id = id;
      return this;
    }

    public ManagerBuilder settings(GossipSettings settings) {
      this.settings = settings;
      return this;
    }

    public ManagerBuilder gossipMembers(List<GossipMember> members) {
      this.gossipMembers = members;
      return this;
    }

    public ManagerBuilder listener(GossipListener listener) {
      this.listener = listener;
      return this;
    }
    
    public ManagerBuilder registry(MetricRegistry registry) {
      this.registry = registry;
      return this;
    }

    public ManagerBuilder uri(URI uri){
      this.uri = uri;
      return this;
    }
    
    public ManagerBuilder mapper(ObjectMapper objectMapper){
      this.objectMapper = objectMapper;
      return this;
    }

    public ManagerBuilder messageInvoker(MessageInvoker messageInvoker) {
      this.messageInvoker = messageInvoker;
      return this;
    }

    public RandomGossipManager build() {
      checkArgument(id != null, "You must specify an id");
      checkArgument(cluster != null, "You must specify a cluster name");
      checkArgument(settings != null, "You must specify gossip settings");
      checkArgument(uri != null, "You must specify a uri");
      checkArgument(registry != null, "You must specify a MetricRegistry");
      if (properties == null){
        properties = new HashMap<String,String>();
      }
      if (listener == null){
        listener((a,b) -> {});
      }
      if (gossipMembers == null) {
        gossipMembers = new ArrayList<>();
      }
      if (objectMapper == null) {
        objectMapper = new ObjectMapper();
        objectMapper.enableDefaultTyping();
        objectMapper.registerModule(new CrdtModule());
        objectMapper.configure(Feature.WRITE_NUMBERS_AS_STRINGS, false);
      }
      if (messageInvoker == null) {
        messageInvoker = new DefaultMessageInvoker();
      }
      return new RandomGossipManager(cluster, uri, id, properties, settings, gossipMembers, listener, registry, objectMapper, messageInvoker);
    }
  }

  private RandomGossipManager(String cluster, URI uri, String id, Map<String,String> properties,  GossipSettings settings, 
          List<GossipMember> gossipMembers, GossipListener listener, MetricRegistry registry, ObjectMapper objectMapper, MessageInvoker messageInvoker) {
    super(cluster, uri, id, properties, settings, gossipMembers, listener, registry, objectMapper, messageInvoker);
  }
}
