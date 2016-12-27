package org.apache.flink.quickstart;

import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class MyMaxWithState extends
		RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

	private static final long serialVersionUID = 1L;

	private ValueStateDescriptor<Map<String, Tuple2<String, Integer>>> stateDescriptor;

	@Override
	public void apply(Tuple key, TimeWindow window,
			Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> collector)
			throws Exception {
		
		// New values
		Iterator<Tuple2<String, Integer>> it = input.iterator();
		// State
		ValueState<Map<String, Tuple2<String, Integer>>> state = getRuntimeContext()
				.getState(stateDescriptor);
		// Values
		Map<String, Tuple2<String, Integer>> values = state.value();

		Tuple2<String, Integer> maxResult = values.get(key.toString());

		while (it.hasNext()) {
			Tuple2<String, Integer> tuple = it.next();

			if (maxResult == null) {
				maxResult = tuple;
			}

			if (tuple.f1 > maxResult.f1) {
				maxResult = tuple;
			}
		}

		values.put(key.toString(), maxResult);

		state.update(values);

		collector.collect(maxResult);
	}

	@Override
	public void open(Configuration conf) {
		this.stateDescriptor = new ValueStateDescriptor<>(
				"last-result", new TypeHint<Map<String, Tuple2<String, Integer>>>() {
				}.getTypeInfo(), new HashMap<String, Tuple2<String, Integer>>());
	}

}
