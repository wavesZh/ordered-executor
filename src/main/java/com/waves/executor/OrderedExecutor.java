package com.waves.executor;

import com.waves.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author taozhang
 */
public class OrderedExecutor {

	private interface Relation<RID> {
		RID getId();
	}


	public static abstract class OrderdRunnable<RID> implements Runnable, Relation<RID> {
	}

	/**
	 *
	 * 限制了消费者数量， 每个ringbuffer只能由一个线程消费。无锁。
	 */
	private RingBuffer[] ringBuffers;

	private Map<RingBuffer, Future> futureMap;

	private ExecutorService[] executorServices;


	public OrderedExecutor(int disruptorNum, int ringBufferNum, int bufferSize) {
		// executor数量需要跟ringbuffers数量一致
		executorServices = new ExecutorService[ringBufferNum];
		for (int i=0; i<ringBufferNum; i++) {
			executorServices[i] = Executors.newSingleThreadExecutor();
		}
		initRingBuffers(ringBufferNum, bufferSize);
		futureMap = new HashMap<>(ringBufferNum);
		startUpDispatcher(disruptorNum);
	}

	private void initRingBuffers(int ringBufferNum, int bufferSize) {
		ringBuffers = new RingBuffer[ringBufferNum];
		for (int i = 0; i < ringBufferNum; i++) {
			ringBuffers[i] = new RingBuffer(bufferSize);
		}
	}

	/**
	 * 提交任务.
	 * 其实也可以考虑使用多个single thread pool： thread正在处理，就把task放入queue中。
	 * @param task
	 */
	public void submit(OrderdRunnable<Object> task) {
		Object rid = task.getId();
		RingBuffer ringBuffer = selectRingBuffer(rid);
		ringBuffer.publish(task);

	}

	private RingBuffer selectRingBuffer(Object rid) {
		return ringBuffers[getHashId(rid)];
	}

	private int getHashId(Object rid) {
		return rid.hashCode() % ringBuffers.length;
	}

	/**
	 * 分发任务。
	 *
	 * @param num dispatcher的数量
	 */
	private void startUpDispatcher(int num) {

		for (int i = 0; i < num; i++) {
			int finalI = i;
			new Thread(() -> {
				for (; ;) {
					try {
						int index = finalI;
						while (index < ringBuffers.length) {
							RingBuffer ringBuffer = ringBuffers[index];
							Future future = futureMap.get(ringBuffer);

							if (future == null || future.isDone()) {
								Object object = ringBuffer.get();
								if (object != null) {
									OrderdRunnable orderdRunnable = (OrderdRunnable) object;
									Object id = orderdRunnable.getId();
									System.out.println(Thread.currentThread().getName() + " : " + id);
									Runnable task = (Runnable) object;
									future = executorServices[getHashId(id)].submit(task);
									futureMap.put(ringBuffer, future);
								}
							} else {

							}
							index += num;
						}
					} catch (Throwable t) {

					}
				}
			}, "dispatcher-" + finalI + "-" + this.hashCode()).start();

		}
	}
}
