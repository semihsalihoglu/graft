package stanford.infolab.debugger.examples.simpledebug;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

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
  }

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR, LongSumAggregator.class);
  }
}