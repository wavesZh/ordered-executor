package com.waves.executor;

import org.junit.Test;

import static org.junit.Assert.*;

public class OrderedExecutorTest {

	@Test
	public void testSingleThread() {
		OrderedExecutor orderedExecutor = new OrderedExecutor(1, 1, 2);
		orderedExecutor.submit(new OrderedExecutor.OrderdRunnable<Object>() {
			@Override
			public Object getId() {
				return "1";
			}

			@Override
			public void run() {
				System.out.println("test");
			}
		});
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMultiThread() {
		OrderedExecutor orderedExecutor = new OrderedExecutor(2, 4, 1);

		for (int i=0; i<10; i++) {
			int finalI = i;
			orderedExecutor.submit(new OrderedExecutor.OrderdRunnable<Object>() {
				@Override
				public void run() {
					try {
						Thread.sleep((int)(Math.random()*90 + 10));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
//					System.out.println(Thread.currentThread().getName() + " : " + finalI);
				}

				@Override
				public Object getId() {
					return finalI;
				}

				@Override
				public String toString() {
					return finalI + "";
				}
			});
		}

		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


}