package com.waves.ringbuffer;


public class EventHolder<T> {

	private T event;

	private long sequence;

	public EventHolder() {
	}

	public EventHolder(T event, long sequence) {
		this.event = event;
		this.sequence = sequence;
	}


	public T getEvent() {
		return event;
	}

	public void setEvent(T event) {
		this.event = event;
	}

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}
}
