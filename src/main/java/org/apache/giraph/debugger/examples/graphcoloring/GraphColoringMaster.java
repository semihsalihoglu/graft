package org.apache.giraph.debugger.examples.graphcoloring;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class GraphColoringMaster extends DefaultMasterCompute {

  public static final String CYCLE = "cycle";
  public static final String NUM_VERTICES_COLORED = "numVerticesColored";
  public static final String NUM_VERTICES_NOT_IN_SET = "numVerticesNotInSet";

  @Override
  public void initialize() throws InstantiationException,
    IllegalAccessException {
    registerPersistentAggregator(NUM_VERTICES_NOT_IN_SET,
      LongSumAggregator.class);
    registerPersistentAggregator(NUM_VERTICES_COLORED, LongSumAggregator.class);
    registerPersistentAggregator(CYCLE, IntMaxAggregator.class);
  }

  @Override
  public void compute() {
    long numColored = ((LongWritable) getAggregatedValue(NUM_VERTICES_COLORED))
      .get();
    long numNotInSet = ((LongWritable) getAggregatedValue(NUM_VERTICES_NOT_IN_SET))
      .get();
    if (numColored + numNotInSet == getTotalNumVertices()) {
      if (numNotInSet == 0) {
        // Halt when all vertices are colored.
        haltComputation();
      } else {
        // Start a new cycle of finding independent sets.
        IntWritable cycle = (IntWritable) getAggregatedValue(CYCLE);
        cycle.set(cycle.get() + 1);
        setAggregatedValue(CYCLE, cycle);
      }
    }
  }
}
