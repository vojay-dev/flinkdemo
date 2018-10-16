package com.innogames.analytics.flinkdemo.volume;

import com.innogames.analytics.flinkdemo.volume.source.VolumeLevel;
import com.innogames.analytics.flinkdemo.volume.source.VolumeLevelSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Show only amplitudes greater than X
 */
public class FilterApp {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<VolumeLevel> stream = env.addSource(new VolumeLevelSourceFunction());

		stream
			.filter(volumeLevel -> volumeLevel.getAmplitude() >= 0.8)
			.print();

		env.execute("volume stream");
	}

}
