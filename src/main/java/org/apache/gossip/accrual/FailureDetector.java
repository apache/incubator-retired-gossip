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
package org.apache.gossip.accrual;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ExponentialDistributionImpl;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.gossip.LocalMember;
import org.apache.log4j.Logger;

public class FailureDetector {

  private static final Logger LOGGER = Logger.getLogger(FailureDetector.class);
  private final DescriptiveStatistics descriptiveStatistics;
  private final long minimumSamples;
  private volatile long latestHeartbeatMs = -1;
  private final LocalMember parent;
  private final String distribution;
  
  public FailureDetector(LocalMember parent, long minimumSamples, int windowSize, String distribution){
    this.parent = parent;
    descriptiveStatistics = new DescriptiveStatistics(windowSize);
    this.minimumSamples = minimumSamples;
    this.distribution = distribution;
  }
  
  /**
   * Updates the statistics based on the delta between the last 
   * heartbeat and supplied time
   * @param now the time of the heartbeat in milliseconds
   */
  public void recordHeartbeat(long now){
    if (now < latestHeartbeatMs)
      return;
    if (now - latestHeartbeatMs == 0){
      return;
    }
    synchronized (descriptiveStatistics) {
      if (latestHeartbeatMs != -1){
        descriptiveStatistics.addValue(now - latestHeartbeatMs);
      } else {
        latestHeartbeatMs = now;
      }
    }
  }
  
  public Double computePhiMeasure(long now)  {
    if (latestHeartbeatMs == -1 || descriptiveStatistics.getN() < minimumSamples) {
      LOGGER.debug(
              String.format( "%s latests %s samples %s minumumSamples %s", parent.getId(), 
                      latestHeartbeatMs, descriptiveStatistics.getN(), minimumSamples));
      return null;
    }
    synchronized (descriptiveStatistics) {
      long delta = now - latestHeartbeatMs;
      try {
        double probability = 0.0;
        if (distribution.equals("normal")){
          double variance = descriptiveStatistics.getVariance();
          probability = 1.0d - new NormalDistributionImpl(descriptiveStatistics.getMean(), 
                  variance == 0 ? 0.1 : variance).cumulativeProbability(delta);
        } else {
          probability = 1.0d - new ExponentialDistributionImpl(descriptiveStatistics.getMean()).cumulativeProbability(delta);
        }
        return -1.0d * Math.log10(probability);
      } catch (MathException | IllegalArgumentException e) {
        StringBuilder sb = new StringBuilder();
        for ( double d: descriptiveStatistics.getSortedValues()){
          sb.append(d + " ");
        }
        LOGGER.debug(e.getMessage() +" "+ sb.toString() +" "+ descriptiveStatistics);
        throw new IllegalArgumentException(e);
      }
    }
  }
}
