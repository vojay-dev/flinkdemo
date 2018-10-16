package com.innogames.analytics.flinkdemo.twitter;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class App {

	private static final String SEARCH_TERM = "igcsdemo";

	public static void main(String[] args) throws Exception {
		final Properties config = new Properties();
		config.load(Resources.asCharSource(
			Resources.getResource("config.properties"),
			Charsets.UTF_8
		).openStream());

		final TwitterSource twitterSource = new TwitterSource(config);
		twitterSource.setCustomEndpointInitializer(new CustomFilterEndpointInitializer(SEARCH_TERM));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<String> stream = env.addSource(twitterSource);

		stream
			.map(new TwitterMapFunction())
				.setParallelism(2)
				.name("map tweets to tuples")
			.filter((FilterFunction<Tuple2<String, String>>) tuple -> tuple.f0 != null && tuple.f1 != null)
				.setParallelism(2)
				.name("filter empty tweets")
			.print()
				.setParallelism(1)
				.name("print to stdout");

		env.execute("twitter demo");
	}

}
