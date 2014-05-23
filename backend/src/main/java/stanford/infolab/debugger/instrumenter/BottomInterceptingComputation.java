package stanford.infolab.debugger.instrumenter;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BottomInterceptingComputation<I extends WritableComparable, V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
		extends UserComputation<I, V, E, M1, M2> {

	@Intercept
	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
			GraphTaskManager<I, V, E> graphTaskManager,
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

	@Intercept
	@Override
	public final void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
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

	@Intercept
	@Override
	public void preSuperstep() {
		interceptPreSuperstepBegin();
		super.preSuperstep();
	}

	@Intercept
	@Override
	public void postSuperstep() {
		super.postSuperstep();
		interceptPostSuperstepEnd();
	}

	@Override
	public Class getActualTestedClass() {
		return getClass();
	}

}
