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

package com.intel.flinkbench;

import com.intel.flinkbench.microbench.*;
import com.intel.flinkbench.util.BenchLogUtil;
import com.intel.hibench.streambench.common.ConfigLoader;
import com.intel.flinkbench.util.FlinkBenchConfig;

public class RunBench {
    public static void main(String[] args) throws Exception {
        runAll(args);
    }

    public static void runAll(String[] args) throws Exception {

        if (args.length < 2)
            BenchLogUtil.handleError("Usage: RunBench <ConfigFile> <FrameworkName>");

        FlinkBenchConfig conf = new FlinkBenchConfig();

        ConfigLoader cl = new ConfigLoader(args[0]);

        conf.master = cl.getProperty("hibench.streamingbench.brokerList");
        conf.zkHost = cl.getProperty("hibench.streamingbench.zookeeper.host");
        conf.workerCount = Integer.parseInt(cl.getProperty("hibench.streamingbench.storm.worker_count"));
        conf.benchName = cl.getProperty("hibench.streamingbench.benchname");
        conf.topic = cl.getProperty("hibench.streamingbench.topic_name");
        conf.consumerGroup = cl.getProperty("hibench.streamingbench.consumer_group");
        
        String benchName = conf.benchName;

        BenchLogUtil.logMsg("Benchmark starts.." + benchName +
                "   Frameworks:" + "Flink");

        if (benchName.equals("wordcount")) {
            conf.separator = cl.getProperty("hibench.streamingbench.separator");
            WordCount wordCount = new WordCount();
            wordCount.processStream(conf);
        } else if (benchName.equals("identity")) {
            Identity identity = new Identity();
            identity.processStream(conf);
        } else if (benchName.equals("sample")) {
            conf.prob = Double.parseDouble(cl.getProperty("hibench.streamingbench.prob"));
            Sample sample = new Sample();
            sample.processStream(conf);
        } else if (benchName.equals("project")) {
            conf.separator = cl.getProperty("hibench.streamingbench.separator");
            conf.fieldIndex = Integer.parseInt(cl.getProperty("hibench.streamingbench.field_index"));
            Projection project = new Projection();
            project.processStream(conf);
        } else if (benchName.equals("grep")) {
            conf.pattern = cl.getProperty("hibench.streamingbench.pattern");
            Grep grep = new Grep();
            grep.processStream(conf);
        } else if (benchName.equals("distinctcount")) {
            DistinctCount distinct = new DistinctCount();
            distinct.processStream(conf);
        } else if (benchName.equals("statistics")) {
            Statistics numeric = new Statistics();
            numeric.processStream(conf);
        }
    }
}
