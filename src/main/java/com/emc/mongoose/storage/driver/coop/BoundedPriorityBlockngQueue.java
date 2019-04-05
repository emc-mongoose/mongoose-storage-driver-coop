package com.emc.mongoose.storage.driver.coop;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class BoundedPriorityBlockngQueue<E>
extends PriorityBlockingQueue<E>
implements BlockingQueue<E> {

	private final int capacity;

	public BoundedPriorityBlockngQueue(final int capacity, final Comparator<E> comparator) {
		super(capacity, comparator);
		this.capacity = capacity;
	}

	@Override
	public boolean offer(final E element) {
		if(size() < capacity) {
			return super.offer(element);
		} else {
			return false;
		}
	}
}
