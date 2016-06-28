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

package com.intel.hibench.streambench.storm;

import com.intel.hibench.streambench.common.ConfigLoader;
import com.intel.hibench.streambench.storm.micro.*;
import com.intel.hibench.streambench.storm.micro.NumericCalc;
import com.intel.hibench.streambench.storm.trident.*;
import com.intel.hibench.streambench.storm.util.BenchLogUtil;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;

public class RunBench {

  public static void main(String[] args) throws Exception {
    runAll(args);
  }

  public static void runAll(String[] args) throws Exception {

    if (args.length < 2)
      BenchLogUtil.handleError("Usage: RunBench <ConfigFile> <FrameworkName>");

    StormBenchConfig conf = new StormBenchConfig();

    ConfigLoader cl = new ConfigLoader(args[0]);
    boolean TridentFramework = false;
    if (args[1].equals("trident")) TridentFramework = true;

    conf.nimbus = cl.getProperty("hibench.streamingbench.storm.nimbus");
    conf.nimbusAPIPort = Integer.parseInt(cl.getProperty("hibench.streamingbench.storm.nimbusAPIPort"));
    conf.zkHost = cl.getProperty("hibench.streamingbench.zookeeper.host");
    conf.workerCount = Integer.parseInt(cl.getProperty("hibench.streamingbench.storm.worker_count"));
    conf.spoutThreads = Integer.parseInt(cl.getProperty("hibench.streamingbench.storm.spout_threads"));
    conf.boltThreads = Integer.parseInt(cl.getProperty("hibench.streamingbench.storm.bolt_threads"));
    conf.benchName = cl.getProperty("hibench.streamingbench.benchname");
    conf.recordCount = Long.parseLong(cl.getProperty("hibench.streamingbench.record_count"));
    conf.topic = cl.getProperty("hibench.streamingbench.topic_name");
    conf.consumerGroup = cl.getProperty("hibench.streamingbench.consumer_group");
    conf.readFromStart = Boolean.parseBoolean(cl.getProperty("hibench.streamingbench.storm.read_from_start"));
    conf.ackon = Boolean.parseBoolean(cl.getProperty("hibench.streamingbench.storm.ackon"));
    conf.nimbusContactInterval = Integer.parseInt(cl.getProperty("hibench.streamingbench.storm.nimbusContactInterval"));

    String benchName = conf.benchName;

    BenchLogUtil.logMsg("Benchmark starts... " + "  " + benchName +
            "   Frameworks:" + (TridentFramework ? "Trident" : "Storm"));

    if (TridentFramework) { // For trident workloads
      if (benchName.equals("wordcount")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        TridentWordcount wordcount = new TridentWordcount(conf);
        wordcount.run();
      } else if (benchName.equals("identity")) {
        TridentIdentity identity = new TridentIdentity(conf);
        identity.run();
      } else if (benchName.equals("sample")) {
        conf.prob = Double.parseDouble(cl.getProperty("hibench.streamingbench.prob"));
        TridentSample sample = new TridentSample(conf);
        sample.run();
      } else if (benchName.equals("project")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        TridentProject project = new TridentProject(conf);
        project.run();
      } else if (benchName.equals("grep")) {
        conf.pattern = cl.getProperty("hibench.streamingbench.pattern");
        TridentGrep grep = new TridentGrep(conf);
        grep.run();
      } else if (benchName.equals("distinctcount")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        TridentDistinctCount distinct = new TridentDistinctCount(conf);
        distinct.run();
      } else if (benchName.equals("statistics")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        TridentNumericCalc numeric = new TridentNumericCalc(conf);
        numeric.run();
      }
    } else { // For storm workloads
      if (benchName.equals("identity")) {
        Identity identity = new Identity(conf);
        identity.run();
      } else if (benchName.equals("project")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        ProjectStream project = new ProjectStream(conf);
        project.run();
      } else if (benchName.equals("sample")) {
        conf.prob = Double.parseDouble(cl.getProperty("hibench.streamingbench.prob"));
        SampleStream sample = new SampleStream(conf);
        sample.run();
      } else if (benchName.equals("wordcount")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        Wordcount wordcount = new Wordcount(conf);
        wordcount.run();
      } else if (benchName.equals("grep")) {
        conf.pattern = cl.getProperty("hibench.streamingbench.pattern");
        GrepStream grep = new GrepStream(conf);
        grep.run();
      } else if (benchName.equals("statistics")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        NumericCalc numeric = new NumericCalc(conf);
        numeric.run();
      } else if (benchName.equals("distinctcount")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        DistinctCount distinct = new DistinctCount(conf);
        distinct.run();
      } else if (benchName.equals("statistics")) {
        conf.separator = cl.getProperty("hibench.streamingbench.separator");
        conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
        NumericCalcSep numeric = new NumericCalcSep(conf);
        numeric.run();
      }
    }
  }
}
