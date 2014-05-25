package stanford.infolab.debugger.examples.simpledebug;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

import stanford.infolab.debugger.examples.exceptiondebug.SimpleTriangleClosingActualComputation;
import stanford.infolab.debugger.examples.integrity.ConnectedComponentsActualComputation;

/**
 * Master compute associated with {@link RandomWalkComputation}. It handles
 * dangling nodes.
 */
public class SimpleShortestPathsMaster extends DefaultMasterCompute {

  public static String NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR = "nvWithDistanceLessThanThree";

  @SuppressWarnings("unchecked")
@Override
  public void compute() {
    System.out.println("Running SimpleShortestPathsMaster.compute. superstep " + getSuperstep());
    LongWritable aggregatorValue = getAggregatedValue(NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR);
    if (aggregatorValue != null) {
      System.out.println("At Master.compute() with aggregator: " + aggregatorValue.get());
    }
    if (getSuperstep() == 100) {
    	setComputation(ConnectedComponentsActualComputation.class);
    	try {
			setComputation((Class<? extends Computation>) Class.forName("stanford.infolab.debugger.examples.exceptiondebug.SimpleTriangleClosingActualComputation"));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
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