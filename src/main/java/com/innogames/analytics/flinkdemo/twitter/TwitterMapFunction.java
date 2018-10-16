package com.innogames.analytics.flinkdemo.twitter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class TwitterMapFunction implements MapFunction<String, Tuple2<String, String>> {

	@Override
	public Tuple2<String, String> map(final String json) {
		try {
			final JsonNode tweet = new ObjectMapper().readTree(json);

			final String user = tweet.get("user").get("name").textValue();
			final String text = tweet.get("text").textValue().replace("\n", "");

			return new Tuple2<>(user, text);
		} catch(Exception e) {
			return new Tuple2<>(null, null);
		}
	}

}
