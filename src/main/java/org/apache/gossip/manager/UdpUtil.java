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
package org.apache.gossip.manager;

import java.nio.ByteBuffer;

public class UdpUtil {

  public static int readPacketLengthFromBuffer(byte [] buffer){
    int packetLength = 0;
    for (int i = 0; i < 4; i++) {
      int shift = (4 - 1 - i) * 8;
      packetLength += (buffer[i] & 0x000000FF) << shift;
    }
    return packetLength;
  }
  
  public static byte[] createBuffer(int packetLength, byte[] jsonBytes) {
    byte[] lengthBytes = new byte[4];
    lengthBytes[0] = (byte) (packetLength >> 24);
    lengthBytes[1] = (byte) ((packetLength << 8) >> 24);
    lengthBytes[2] = (byte) ((packetLength << 16) >> 24);
    lengthBytes[3] = (byte) ((packetLength << 24) >> 24);
    ByteBuffer byteBuffer = ByteBuffer.allocate(4 + jsonBytes.length);
    byteBuffer.put(lengthBytes);
    byteBuffer.put(jsonBytes);
    byte[] buf = byteBuffer.array();
    return buf;
  }
}
