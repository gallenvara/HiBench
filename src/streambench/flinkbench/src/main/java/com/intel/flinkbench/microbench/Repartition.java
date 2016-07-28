package com.intel.flinkbench.microbench;

import com.intel.flinkbench.datasource.StreamBase;
import com.intel.flinkbench.util.FlinkBenchConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.intel.hibench.streambench.common.metrics.KafkaReporter;

public class Repartition extends StreamBase {
    
    @Override
    public void processStream(final KafkaReporter kafkaReporter, FlinkBenchConfig config) throws Exception{

        createDataStream(config);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> dataStream = env.addSource(getDataStream());
        dataStream.rebalance().map((new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
                kafkaReporter.report(Long.parseLong(value.f0), System.currentTimeMillis());
                return value;
            }
        }));
        env.execute("Repartition job");
    }
}
