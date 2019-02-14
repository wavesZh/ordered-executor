package com.waves.executor;

import com.waves.ringbuffer.RingBuffer;

import java.util.Map;
import java.util.concurrent.*;

/**
 * @author taozhang
 */
public class OrderedExecutor extends ThreadPoolExecutor {

	private static interface Relation<RID> {
		RID getId();
	}


	public static abstract class OrderdRunnable<RID> implements Runnable, Relation<RID> {
		/**
		 * 绑定线程， 线程的名称
		 */
		private String dispatcher;

		public String getDispatcher() {
			return dispatcher;
		}

		public void setDispatcher(String dispatcher) {
			this.dispatcher = dispatcher;
		}
	}

	/**
	 * 限制了消费者数量， 每个ringbuffer只能由一个线程消费。无锁。
	 */
	private RingBuffer[] ringBuffers;


	private Map<RingBuffer, Future> futureMap;

	/**
	 * 提交任务.
	 * 其实也可以考虑使用多个single thread pool： 正在执行task，就把task放入queue中。
	 * @param task
	 */
	public void submit(OrderdRunnable<Object> task) {
		Object rid = task.getId();
		RingBuffer ringBuffer = selectRingBuffer(rid);
		ringBuffer.publish(task);

//		ExecutorService executor = Executors.newSingleThreadExecutor();
//		executor.submit(task);
	}

	private RingBuffer selectRingBuffer(Object rid) {
		return ringBuffers[getHashId(rid)];
	}

	private int getHashId(Object rid) {
		return rid.hashCode() % ringBuffers.length;
	}

	/**
	 * 单线程分发任务。
	 * // TODO: 2019-02-14 可配置多个dispatcher
	 */
	private void startUpDispatcher() {
		new Thread(()->{
			for (;;) {
				try {
					for (RingBuffer ringBuffer : ringBuffers) {
						Future future = futureMap.get(ringBuffer);
						if(future.isDone()) {
							Runnable task = (Runnable) ringBuffer.get();
							this.submit(task);
						} else {
//							System.out.println("");
						}
					}
				} catch (Throwable t) {

				}
			}
		}).start();
	}

	public OrderedExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}

	public OrderedExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
	}

	public OrderedExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
	}

	public OrderedExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
	}
}
