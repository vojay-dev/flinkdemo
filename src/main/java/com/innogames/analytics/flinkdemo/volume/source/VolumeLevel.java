package com.innogames.analytics.flinkdemo.volume.source;

import com.google.common.base.MoreObjects;

public class VolumeLevel {

	private final float amplitude;
	private final float peak;

	public VolumeLevel(final float amplitude, final float peak) {
		this.amplitude = amplitude;
		this.peak = peak;
	}

	public float getAmplitude() {
		return amplitude;
	}

	public float getPeak() {
		return peak;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
			.add("amplitude", amplitude)
			.add("peak", peak)
			.toString();
	}

}
