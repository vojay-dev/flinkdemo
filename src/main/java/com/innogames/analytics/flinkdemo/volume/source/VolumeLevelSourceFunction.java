package com.innogames.analytics.flinkdemo.volume.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.LineUnavailableException;
import javax.sound.sampled.TargetDataLine;

public class VolumeLevelSourceFunction implements SourceFunction<VolumeLevel> {

	private volatile boolean running;

	@Override
	public void run(final SourceContext<VolumeLevel> context) {
		running = true;

		while(running) {
			final AudioFormat audioFormat = new AudioFormat(44100f, 16, 1, true, false);
			final int bufferByteSize = 2048;

			TargetDataLine line;

			try {
				line = AudioSystem.getTargetDataLine(audioFormat);
				line.open(audioFormat, bufferByteSize);
			} catch(final LineUnavailableException e) {
				System.err.println(e);
				return;
			}

			final byte[] buffer = new byte[bufferByteSize];
			final float[] samples = new float[bufferByteSize / 2];

			float lastPeak = 0f;

			line.start();
			for(int b; (b = line.read(buffer, 0, buffer.length)) > -1; ) {
				// convert bytes to samples here
				for(int i = 0, s = 0; i < b; ) {
					int sample = 0;

					sample |= buffer[i++] & 0xFF; // reverse these two lines
					sample |= buffer[i++] << 8;   // if the format is big endian

					// normalize to range of +/-1.0f
					samples[s++] = sample / 32768f;
				}

				float rms = 0f;
				float peak = 0f;

				for(final float sample : samples) {
					final float abs = Math.abs(sample);
					if(abs > peak) {
						peak = abs;
					}

					rms += sample * sample;
				}

				rms = (float) Math.sqrt(rms / samples.length);

				if(lastPeak > peak) {
					peak = lastPeak * 0.875f;
				}

				lastPeak = peak;

				context.collect(new VolumeLevel(rms, peak));
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

}
