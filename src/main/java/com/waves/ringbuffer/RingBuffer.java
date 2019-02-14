package com.waves.ringbuffer;


import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

abstract class RingBufferPad {
	protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFields<E> extends RingBufferPad {
	private final EventHolder<E>[] entries;
	protected final int bufferSize;
	// head： current consume    tail: current product
	protected AtomicLong head, tail;
	//	private static final long REF_ARRAY_BASE;
	//	private static final int REF_ELEMENT_SHIFT;
	//
	//	static
	//	{
	//		final int scale = UNSAFE.arrayIndexScale(Object[].class);
	//		if (4 == scale)
	//		{
	//			REF_ELEMENT_SHIFT = 2;
	//		}
	//		else if (8 == scale)
	//		{
	//			REF_ELEMENT_SHIFT = 3;
	//		}
	//		else
	//		{
	//			throw new IllegalStateException("Unknown pointer size");
	//		}
	//		BUFFER_PAD = 128 / scale;
	//		// Including the buffer pad in the array base offset
	//		REF_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
	//	}


	protected RingBufferFields(int bufferSize) {
		this.bufferSize = bufferSize;
		// TODO: 2018/12/7
		entries = new EventHolder[bufferSize];
		fill();
		head = new AtomicLong(-1);
		tail = new AtomicLong(-1);
	}

	private void fill() {
		for (int i = 0; i < bufferSize; i++) {
			entries[i] = new EventHolder<>();
		}
	}

	protected final EventHolder<E> elementAt(long sequence) {
		// TODO: 2018/12/8
		long index = sequence % bufferSize;
		return entries[(int) index];
	}
}

public final class RingBuffer<E> extends RingBufferFields<E> {

	protected long p1, p2, p3, p4, p5, p6, p7;


	public RingBuffer(int bufferSize) {
		super(bufferSize);
	}

	public void publish(List<E> events) {

		long next = next(events.size());
		long start = next - (events.size() - 1);

		for (long i = start; start < next; start++) {
			publish(events.get((int) i), i);
		}
	}

	public void publish(E event) {
		long next = next(1);
		publish(event, next);
	}


	public void publish(E event, long sequence) {
		EventHolder<E> holder = elementAt(sequence);
		holder.setSequence(sequence);
		holder.setEvent(event);
	}


	public E get() {
		if (tail.get() <= head.get()) {
			return null;
		}
		long next = head.incrementAndGet();
		return elementAt(next).getEvent();
	}

	/**
	 * 做CAS同步处理
	 * @param n
	 * @return
	 */
	public long next(int n) {
		long next;
		do {
			// 当前生产的序列
			long current = tail.get();

			next = current + n;

			long wrapPoint = next - bufferSize;

			// 生产快于消费，将发生绕圈覆盖
			if (wrapPoint > head.get()) {
				// wait or throw exception or discard slice
			} else if (tail.compareAndSet(current, next)) {
				break;
			}
		} while (true);

		return next;
	}

}

