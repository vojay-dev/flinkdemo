package com.innogames.analytics.flinkdemo.sql;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.innogames.analytics.flinkdemo.twitter.CustomFilterEndpointInitializer;
import com.innogames.analytics.flinkdemo.twitter.TwitterMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.Properties;

public class AdvancedSqlApp {

	private static final String SEARCH_TERM = "music";

	public static void main(String[] args) throws Exception {
		final Properties config = new Properties();
		config.load(Resources.asCharSource(
			Resources.getResource("config.properties"),
			Charsets.UTF_8
		).openStream());

		final TwitterSource twitterSource = new TwitterSource(config);
		twitterSource.setCustomEndpointInitializer(new CustomFilterEndpointInitializer(SEARCH_TERM));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		final DataStream<String> stream = env.addSource(twitterSource);

		final SingleOutputStreamOperator<Tuple2<String, String>> tweets = stream
			.map(new TwitterMapFunction())
			.setParallelism(2)
			.name("map tweets to tuples");

		final Table table = tableEnv.fromDataStream(tweets, "user, text, proctime.proctime");

		final Table result = tableEnv.sqlQuery(
			"SELECT TUMBLE_START(proctime, INTERVAL '10' SECOND), COUNT(*) " +
			"FROM " + table + " " +
			"WHERE text LIKE '%rock%' " +
			"GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND)"
		);

		result.writeToSink(new CsvTableSink("/tmp/x", "|", 1, FileSystem.WriteMode.OVERWRITE));

		env.execute("sql demo");
	}

}
