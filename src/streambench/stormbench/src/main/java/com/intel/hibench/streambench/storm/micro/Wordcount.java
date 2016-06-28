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
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class Wordcount extends SingleSpoutTops {

  public Wordcount(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setBolts(TopologyBuilder builder) {
    builder.setBolt("split", new SplitStreamBolt(config.separator),
            config.boltThreads).shuffleGrouping("spout");
    builder.setBolt("count", new WordCountBolt(),
            config.boltThreads).fieldsGrouping("split", new Fields("word"));
  }

  private static class WordCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      //BenchLogUtil.logMsg("Word:"+word+"  count:"+count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  private static class SplitStreamBolt extends BaseBasicBolt {
    private String separator;

    public SplitStreamBolt(String separator) {
      this.separator = separator;
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String record = tuple.getString(0);
      String[] fields = record.split(separator);
      for (String s : fields) {
        collector.emit(new Values(s));
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

}
