package com.innogames.analytics.flinkdemo.volume;

import com.innogames.analytics.flinkdemo.volume.source.VolumeLevel;
import com.innogames.analytics.flinkdemo.volume.source.VolumeLevelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Print the maximum amplitude in windows of 3 seconds
 */
public class WindowApp {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<VolumeLevel> stream = env.addSource(new VolumeLevelSourceFunction());

		stream
			.map(VolumeLevel::getAmplitude)
			.timeWindowAll(Time.seconds(3))
			.max(0)
			.print();

		env.execute("volume stream");
	}

}
