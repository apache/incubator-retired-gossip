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
package com.google.code.gossip;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.json.JSONException;

public class GossipRunner {

  public static void main(String[] args) {
    File configFile;
    if (args.length == 1) {
      configFile = new File("./" + args[0]);
    } else {
      configFile = new File("gossip.conf");
    }
    new GossipRunner(configFile);
  }

  public GossipRunner(File configFile) {
    if (configFile != null && configFile.exists()) {
      try {
        System.out.println("Parsing the configuration file...");
        StartupSettings _settings = StartupSettings.fromJSONFile(configFile);
        GossipService gossipService = new GossipService(_settings);
        System.out.println("Gossip service successfully initialized, let's start it...");
        gossipService.start();
      } catch (FileNotFoundException e) {
        System.err.println("The given file is not found!");
      } catch (JSONException e) {
        System.err.println("The given file is not in the correct JSON format!");
      } catch (IOException e) {
        System.err.println("Could not read the configuration file: " + e.getMessage());
      } catch (InterruptedException e) {
        System.err.println("Error while starting the gossip service: " + e.getMessage());
      }
    } else {
      System.out
              .println("The gossip.conf file is not found.\n\nEither specify the path to the startup settings file or place the gossip.json file in the same folder as the JAR file.");
    }
  }
}
