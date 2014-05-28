package org.apache.giraph.debugger.examples.simpledebug;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleShortestPathsDebugComputationToRun extends
		SimpleShortestPathsDebugComputationModified {

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
