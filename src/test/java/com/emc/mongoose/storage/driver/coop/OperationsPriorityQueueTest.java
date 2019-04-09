package com.emc.mongoose.storage.driver.coop;

import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.OperationImpl;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationsPriorityQueueTest {

	@Test
	public void test()
	throws Exception {
		final var op1 = new OperationImpl<>();
		op1.status(Operation.Status.PENDING);
		final var op2 = new OperationImpl<>();
		op2.status(Operation.Status.ACTIVE);
		final var op3 = new OperationImpl<>();
		op3.status(Operation.Status.SUCC);
		final var op4 = new OperationImpl<>();
		op4.status(Operation.Status.FAIL_IO);
		final BlockingQueue<Operation> opQueue = new PriorityBlockingQueue<>(4, CoopStorageDriverBase::compareOps);
		assertTrue(opQueue.offer(op1));
		assertTrue(opQueue.offer(op2));
		assertTrue(opQueue.offer(op3));
		assertTrue(opQueue.offer(op4));
		final List<Operation> ops = new ArrayList<>(4);
		assertEquals(4, opQueue.drainTo(ops));
		ops.stream().map(Operation::status).forEach(System.out::println);
	}
}
