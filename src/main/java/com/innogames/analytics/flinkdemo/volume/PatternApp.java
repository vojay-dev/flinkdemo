package com.innogames.analytics.flinkdemo.volume;

import com.innogames.analytics.flinkdemo.volume.source.VolumeLevel;
import com.innogames.analytics.flinkdemo.volume.source.VolumeLevelSourceFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Detect an amplitude pattern
 */
public class PatternApp {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<VolumeLevel> stream = env.addSource(new VolumeLevelSourceFunction());

		final Pattern<VolumeLevel, ?> pattern = Pattern.<VolumeLevel>begin("start").where(
			new SimpleCondition<VolumeLevel>() {
				@Override
				public boolean filter(final VolumeLevel volumeLevel) {
					return volumeLevel.getAmplitude() >= 0.8;
				}
			}
		).times(10).followedBy("middle").where(
			new SimpleCondition<VolumeLevel>() {
				@Override
				public boolean filter(final VolumeLevel volumeLevel) {
					return volumeLevel.getAmplitude() < 0.8;
				}
			}
		).times(10).followedBy("end").where(
			new SimpleCondition<VolumeLevel>() {
				@Override
				public boolean filter(final VolumeLevel volumeLevel) {
					return volumeLevel.getAmplitude() > 0.8;
				}
			}
		).times(10);

		final PatternStream<VolumeLevel> patternStream = CEP.pattern(stream, pattern);
		final DataStream<String> result = patternStream.select(new VolumeSelectFunction());

		result.print();
		env.execute("volume stream");
	}

	public static class VolumeSelectFunction implements PatternSelectFunction<VolumeLevel, String> {

		@Override
		public String select(final Map<String, List<VolumeLevel>> map) {
			return "PATTERN DETECTED!";
		}

	}

}
