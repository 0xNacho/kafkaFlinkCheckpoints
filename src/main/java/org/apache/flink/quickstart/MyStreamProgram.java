package org.apache.flink.quickstart;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.FetcherType;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.OffsetStore;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class MyStreamProgram {
	
	public static void main (String [] args) throws Exception{
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.enableCheckpointing(4000);

		env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		

				
		env.addSource(new FlinkKafkaConsumer<String>(
				"test",
				new SimpleStringSchema(),
				properties,
				OffsetStore.FLINK_ZOOKEEPER,
				FetcherType.LEGACY_LOW_LEVEL)
		)
		.map(new MapFunction<String, Tuple2<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> map(String arg) throws Exception {
				String[] line  = arg.split(",");				
				return new Tuple2<String, Integer>(line[0], Integer.parseInt(line[1]));
			}
		})
		.keyBy(0)
			.timeWindow(Time.of(5, TimeUnit.SECONDS))		
		.apply(new MyMaxWithState())
		.print();
		
		
		env.execute("Kafka example");
		
	}
}
