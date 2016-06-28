/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.storm.micro;

import com.intel.hibench.streambench.storm.topologies.SingleSpoutTops;
import com.intel.hibench.streambench.storm.util.BenchLogUtil;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Random;

public class SampleStream extends SingleSpoutTops {

  public SampleStream(StormBenchConfig config) {
    super(config);
  }

  public static ThreadLocal<Random> threadRandom(final long seed) {
    return new ThreadLocal<Random>() {
      @Override
      protected Random initialValue() {
        return new Random(seed);
      }
    };
  }

  @Override
  public void setBolts(TopologyBuilder builder) {
    builder.setBolt("sampleAndPrint", new SampleBolt(config.prob),
            config.boltThreads).shuffleGrouping("spout");
  }

  private static class SampleBolt extends BaseBasicBolt {

    private double probability;
    private int count = 0;
    private ThreadLocal<Random> rand = null;

    public SampleBolt(double prob) {
      probability = prob;
      rand = threadRandom(1);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      double randVal = rand.get().nextDouble();
      if (randVal <= probability) {
        count += 1;
        collector.emit(new Values(tuple.getString(0)));
        BenchLogUtil.logMsg("   count:" + count);
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
  }

}
