package com.innogames.analytics.flinkdemo.volume.sink;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.Properties;

public class TwitterSink implements SinkFunction<String> {

	private final Twitter twitter;

	public TwitterSink() throws TwitterException, IOException {
		final Properties properties = new Properties();
		properties.load(Resources.asCharSource(
			Resources.getResource("config.properties"),
			Charsets.UTF_8
		).openStream());

		final ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setDebugEnabled(true)
			.setOAuthConsumerKey(properties.getProperty(TwitterSource.CONSUMER_KEY))
			.setOAuthConsumerSecret(properties.getProperty(TwitterSource.CONSUMER_SECRET))
			.setOAuthAccessToken(properties.getProperty(TwitterSource.TOKEN))
			.setOAuthAccessTokenSecret(properties.getProperty(TwitterSource.TOKEN_SECRET));

		this.twitter = new TwitterFactory(builder.build()).getInstance();
	}

	@Override
	public void invoke(final String value, final Context context) throws Exception {
		twitter.updateStatus(value);
	}

}
