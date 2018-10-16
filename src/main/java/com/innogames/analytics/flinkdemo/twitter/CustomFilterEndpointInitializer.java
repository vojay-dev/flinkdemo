package com.innogames.analytics.flinkdemo.twitter;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Collections;

public class CustomFilterEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {

	private final String searchTerm;

	public CustomFilterEndpointInitializer(final String searchTerm) {
		this.searchTerm = searchTerm;
	}

	@Override
	public StreamingEndpoint createEndpoint() {
		final StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();
		statusesFilterEndpoint.trackTerms(Collections.singletonList(searchTerm));

		return statusesFilterEndpoint;
	}

}
