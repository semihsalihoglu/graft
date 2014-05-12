package stanford.infolab.debugger.examples.simpledebug;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

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
  }

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR, LongSumAggregator.class);
  }
}