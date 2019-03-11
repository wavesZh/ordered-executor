package com.waves.ringbuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

abstract class RingBufferPad {
	protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFields<E> extends RingBufferPad {
	private final EventHolder<E>[] entries;
	protected final int bufferSize;
	// head： current consume    tail: current product
	protected AtomicLong head, tail;
	/**
	 * 0: pre publish 1: publish over 2. consume over
	 */
	protected AtomicInteger flag = new AtomicInteger(2);
	protected final int[] availableRead;
	protected final int[] availableWrite;

	protected ReadWriteLock lock;

	protected RingBufferFields(int bufferSize) {
		this.bufferSize = bufferSize;
		entries = new EventHolder[bufferSize];
		availableWrite = new int[bufferSize];
		availableRead = new int[bufferSize];
		fill();
		head = new AtomicLong(-1);
		tail = new AtomicLong(-1);
		this.lock = new ReadWriteLock(0);
	}


	private void fill() {
		for (int i = 0; i < bufferSize; i++) {
			entries[i] = new EventHolder<>();
			availableRead[i] = -1;
			availableWrite[i] = -1;
		}
	}

	protected final EventHolder<E> elementAt(long sequence) {
		// TODO: 2018/12/8
		long index = sequence % bufferSize;
		return entries[(int) index];
	}


	protected final void setAvailable(final long sequence, boolean read) {
		int index = (int) (sequence % bufferSize);
		int flag = (int) (sequence / bufferSize);
		int[] availableBuffers = read ? availableRead : availableWrite;
		availableBuffers[index] = flag;
	}

	protected final boolean isAvailable(final long sequence, boolean read) {
		if (sequence == 0 && !read) {
			return true;
		}
		int index = (int) (sequence % bufferSize);
		int flag = (int) (sequence / bufferSize);
		int[] availableBuffers = read ? availableWrite : availableRead;
		return availableBuffers[index] == (read ? flag : flag - 1);
	}
}

public final class RingBuffer<E> extends RingBufferFields<E> {

	protected long p1, p2, p3, p4, p5, p6, p7;

	private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteLock.class);

	public RingBuffer(int bufferSize) {
		super(bufferSize);
	}

	public void publish(E event) {
		long next = next(1);
		lock.lockWrite();
		publish(event, next);
	}


	public void publish(E event, long sequence) {
		EventHolder<E> holder = elementAt(sequence);
		holder.setSequence(sequence);
		holder.setEvent(event);
		setAvailable(sequence, false);
		lock.unlockWrite();
		LOGGER.debug("publish the task[{}] to the ringbuffer[{}] in sequence[{}]", event, this.hashCode(), sequence);
	}

	/**
	 * // TODO: 2019-02-15
	 * @return
	 */
	public E get() {
		// TODO: 2019-02-15 并发问题 当tail>head时， 不一定及时将数据放入，导致取旧数据
		// TODO: 或者head + 1 时并还未拿到event，publish进行了覆盖， 丢失数据。
		if (tail.get() <= head.get()) {
			return null;
		}

		long next;
		while(true) {
			long l = head.get();
			next = l + 1;
			if (isAvailable(next, true) && head.compareAndSet(l, next)) {
				break;
			}
		}
		lock.lockRead();
		E event = elementAt(next).getEvent();
		setAvailable(next, true);
		lock.unlockRead();
		LOGGER.debug("get the task[{}] to the ringbuffer[{}] in sequence[{}]", event, this.hashCode(), next);
		return event;
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
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else if (isAvailable(next, false) && tail.compareAndSet(current, next)) {
				break;
			}
		} while (true);

		return next;
	}

}

