package com.bmc.tsi.ingestor.measures;

public class Measure implements Comparable<Measure> {
	private long timestamp;
	private float value;

	public Measure(float value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	@Override
	public int compareTo(Measure o) {
		return Long.compare(this.timestamp, o.timestamp);
	}

}
