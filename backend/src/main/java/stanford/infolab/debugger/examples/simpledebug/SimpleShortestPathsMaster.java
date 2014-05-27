package stanford.infolab.debugger.examples.simpledebug;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

import stanford.infolab.debugger.examples.exceptiondebug.SimpleTriangleClosingActualComputation;
import stanford.infolab.debugger.instrumenter.AbstractInterceptingMasterCompute;
import stanford.infolab.debugger.instrumenter.BottomInterceptingMasterCompute;

/**
 * Master compute associated with {@link RandomWalkComputation}. It handles
 * dangling nodes.
 */
public class SimpleShortestPathsMaster extends DefaultMasterCompute {

  public static String NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR = "nvWithDistanceLessThanThree";

  @Override
  public void compute() {
    System.out.println("Running SimpleShortestPathsMaster.compute. superstep " + getSuperstep());
    LongWritable aggregatorValue = getAggregatedValue(NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR);
    if (aggregatorValue != null) {
      System.out.println("At Master.compute() with aggregator: " + aggregatorValue.get());
    }
//    if (getSuperstep() == 2) {
//      throw new IllegalArgumentException("DUMMY EXCEPTION FOR TESTING");
//    }

    // Dummy code for testing Instrumenter analysis 
    if (getSuperstep() == 100000) {
    	// which is extremely less likely to happen,
    	setComputation(SimpleTriangleClosingActualComputation.class);
    } else if (getSuperstep() == 200000) {
    	try {
			setComputation((Class<? extends Computation>) Class.forName("stanford.infolab.debugger.examples.integrity.ConnectedComponentsActualComputation"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }
  }

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR, LongSumAggregator.class);
  }
}