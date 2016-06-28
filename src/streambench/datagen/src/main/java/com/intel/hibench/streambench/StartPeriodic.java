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

package com.intel.hibench.streambench;

import com.intel.hibench.streambench.common.ConfigLoader;

import java.io.BufferedReader;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @deprecated replace by DataGenerator
 */
@Deprecated
public class StartPeriodic {

  private static String benchName;
  private static String HDFSMaster;
  private static String dataFile1;
  private static long dataFile1Offset;
  private static String dataFile2;
  private static long dataFile2Offset;


  public static void main(String[] args) {

    if (args.length < 5) {
      System.err.println("args: <ConfigFile> <DATA_FILE1> <DATA_FILE1_OFFSET> <DATA_FILE2> <DATA_FILE2_OFFSET> need to be specified!");
      System.exit(1);
    }

    ConfigLoader cl = new ConfigLoader(args[0]);

    benchName = cl.getProperty("hibench.streamingbench.benchname").toLowerCase();
    String topic = cl.getProperty("hibench.streamingbench.topic_name");
    String brokerList = cl.getProperty("hibench.streamingbench.brokerList");
    int recordPerInterval = Integer.parseInt(cl.getProperty("hibench.streamingbench.prepare.periodic.recordPerInterval"));
    int intervalSpan = Integer.parseInt(cl.getProperty("hibench.streamingbench.prepare.periodic.intervalSpan"));
    int totalRound = Integer.parseInt(cl.getProperty("hibench.streamingbench.prepare.periodic.totalRound"));
    HDFSMaster = cl.getProperty("hibench.hdfs.master");
    dataFile1 = args[1];
    dataFile1Offset = Long.parseLong(args[2]);
    dataFile2 = args[3];
    dataFile2Offset = Long.parseLong(args[4]);
    boolean isNumericData = false;
    if (benchName.contains("statistics")) {
      isNumericData = true;
    }

    KafkaConnector con = new KafkaConnector(brokerList, cl);

    Timer timer = new Timer();
    timer.schedule(new SendTask(totalRound, recordPerInterval, con, topic, isNumericData), 0, intervalSpan);
    System.out.println("Timer scheduled.");
  }

  public static BufferedReader getReader() {
    FileDataGenNew files = new FileDataGenNew(HDFSMaster);
    if (benchName.contains("statistics")) {
      return files.loadDataFromFile(dataFile2, dataFile2Offset);
    } else {
      return files.loadDataFromFile(dataFile1, dataFile1Offset);
    }
  }

  static class SendTask extends TimerTask {
    int leftTimes;
    int recordCount;
    int totalTimes;
    KafkaConnector kafkaCon;
    String topic;
    long totalRecords;
    boolean isNumericData;

    public SendTask(int times, int count, KafkaConnector con, String topic, boolean isNumericData) {
      leftTimes = times;
      recordCount = count;
      totalTimes = times;
      kafkaCon = con;
      this.topic = topic;
      totalRecords = 0;
      this.isNumericData = isNumericData;
    }

    @Override
    public void run() {
      System.out.println("Task run, remains:" + leftTimes);
      if (leftTimes > 0) {
        long recordsSent = 0L;
        while (recordsSent < recordCount) {
        recordsSent += kafkaCon.sendRecords(getReader(), topic, recordCount - recordsSent, isNumericData);
        }
        totalRecords += recordsSent;
        leftTimes--;
      } else {
        System.out.println("Time's up! Total records sent:" + totalRecords);
        kafkaCon.close();
        System.exit(0);
      }
    }
  }
}
