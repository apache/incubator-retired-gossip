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
package org.apache.gossip.examples;

public class ExampleCommon {

  private boolean clearTerminalScreen = true;

  /*
   * Look for -s in args. If there, suppress terminal-clear on write results Shift args for
   * positional args, if necessary
   */
  public String[] checkArgsForClearFlag(String[] args) {
    int pos = 0;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-s")) {
        clearTerminalScreen = false;
      } else {
        // in the case of the -s flag, shift args 
        // down by one slot; this will end up with
        // a duplicate entry in the last position of args,
        // but this is ok, because it will be ignored
        args[pos++] = args[i];
      }
    }
    return args;
  }

  public void optionallyClearTerminal() {
    if (clearTerminalScreen) {
      System.out.print("\033[H\033[2J");
      System.out.flush();
    }
  }
}
