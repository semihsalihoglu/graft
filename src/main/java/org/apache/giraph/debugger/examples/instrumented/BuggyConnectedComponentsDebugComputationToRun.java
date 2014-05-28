package org.apache.giraph.debugger.examples.instrumented;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.debugger.examples.integrity.BuggyConnectedComponentsComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;

/**
 * WARNING: This class is should be used only for development. It is put in the Graft source tree
 * to demonstrate to users the two classes that Graft generates at runtime when instrumenting a
 * {@link Computation} class. This is the example for {@link BuggyConnectedComponentsComputation}.
 * The other class Graft generates is {@link BuggyConnectedComponentsDebugComputationModified}.
 * Please see the Graft documentation for more details on how Graft instruments {@link Computation}
 * classes.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class BuggyConnectedComponentsDebugComputationToRun extends
		BuggyConnectedComponentsDebugComputationModified {

	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor workerClientRequestProcessor,
			GraphTaskManager graphTaskManager,
			WorkerAggregatorUsage workerAggregatorUsage,
			WorkerContext workerContext) {
		try {
			// We first call super.initialize so that the getConf() call below
			// returns a non-null value.
			super.initialize(graphState, workerClientRequestProcessor,
					graphTaskManager, workerAggregatorUsage, workerContext);
		} finally {
			interceptInitializeEnd();
		}
	}

	@Override
	public final void compute(Vertex vertex, Iterable messages)
			throws IOException {
		boolean shouldCatchException = interceptComputeBegin(vertex, messages);
		try {
			if (shouldCatchException) {
				try {
					super.compute(vertex, messages);
				} catch (Exception e) {
					interceptComputeException(vertex, messages, e);
					throw e;
				}
			} else {
				super.compute(vertex, messages);
			}
		} finally {
			interceptComputeEnd(vertex, messages);
		}
	}

	@Override
	public void preSuperstep() {
		interceptPreSuperstepBegin();
		super.preSuperstep();
	}

	@Override
	public void postSuperstep() {
		super.postSuperstep();
		interceptPostSuperstepEnd();
	}

	public Class getActualTestedClass() {
		return getClass().getSuperclass();
	}

}
