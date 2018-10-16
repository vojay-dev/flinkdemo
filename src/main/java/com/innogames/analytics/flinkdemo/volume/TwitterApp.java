package com.innogames.analytics.flinkdemo.volume;

import com.google.common.collect.Ordering;
import com.innogames.analytics.flinkdemo.volume.sink.TwitterSink;
import com.innogames.analytics.flinkdemo.volume.source.VolumeLevel;
import com.innogames.analytics.flinkdemo.volume.source.VolumeLevelSourceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TwitterApp {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<VolumeLevel> stream = env.addSource(new VolumeLevelSourceFunction());

		stream
			.map(VolumeLevel::getAmplitude)
			.timeWindowAll(Time.seconds(3))
			.apply(new RichAllWindowFunction<Float, String, TimeWindow>() {
				private transient ValueState<Boolean> triggered;

				@Override
				public void apply(final TimeWindow window, final Iterable<Float> values, final Collector<String> out) throws Exception {
					final Float max = Ordering.natural().max(values);

					if(!triggered.value() && max >= 0.111) {
						out.collect(String.format("thanks for %.4f", max));
						triggered.update(true);
					}
				}

				@Override
				public void open(Configuration config) {
					ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("triggered", Boolean.class, false);
					triggered = getRuntimeContext().getState(descriptor);
				}
			})
			.addSink(new TwitterSink());

		env.execute("volume stream");
	}

}
