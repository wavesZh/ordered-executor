package com.waves.ringbuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class ReadWriteLock {



	private Sync sync;

	public ReadWriteLock(int state) {
		this.sync = new Sync(state);
	}

	private static class Sync extends AbstractQueuedSynchronizer {

		static final int READ_SHIFT = 16;
		static final int MAX_COUNT = 1 << READ_SHIFT;
		static final int READ_UNIT = (1 << READ_SHIFT);
		static final int WRITE_MASK = (1 << READ_SHIFT) - 1;

		protected Sync(int state) {
			super();
			this.setState(state);
		}

		private static int readCount(int c) {
			return  c >> READ_SHIFT;
		}

		private static int writeCount(int c) {
			return c & WRITE_MASK;
		}

		private static boolean isRead(int acquires) {
			if (acquires <= 0) {
				throw new IllegalArgumentException("acquires must greater than zero!");
			}
			return readCount(acquires) > 0;
		}

		@Override
		protected int tryAcquireShared(int arg) {
			boolean read = isRead(arg);
			for (;;) {
				if (hasQueuedPredecessors()) {
					return -1;
				}
				int state = getState();
				int acquires = read ? readCount(arg) : writeCount(arg);
				if (read) {
					int writeCount = writeCount(state);
					if (writeCount > 0) {
						return -1;
					}
				} else {
					int readCount = readCount(state);
					if (readCount > 0) {
						return -1;
					}
				}
				int available = MAX_COUNT - acquires;
				if (available < 0 || compareAndSetState(state, arg)) {
					return available;
				}
			}
		}

		@Override
		protected boolean tryReleaseShared(int arg) {
			for (;;) {
				int current = getState();
				int next = current - arg;
				//				int count = readCount(current) > 0 ? readCount(arg) : writeCount(arg);
				//				if (next < current)  // overflow
				//					throw new Error("Maximum permit count exceeded");
				if (compareAndSetState(current, next)) {
					return true;
				}
			}
		}
	}

	public void lockWrite() {
		sync.acquireShared(1);
	}

	public void lockRead() {
		sync.acquireShared(Sync.READ_UNIT);
	}

	public void unlockWrite() {
		sync.releaseShared(1);
	}

	public void unlockRead() {
		sync.releaseShared(Sync.READ_UNIT);
	}
}
