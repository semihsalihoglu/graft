package org.apache.giraph.debugger.examples.pagerank;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master compute associated with {@link SimplePageRankComputation}.
 * It registers required aggregators.
 */
public class SimplePageRankMasterCompute extends
    DefaultMasterCompute {
  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerAggregator(SimplePageRankComputation.SUM_AGG, LongSumAggregator.class);
    registerPersistentAggregator(SimplePageRankComputation.MIN_AGG, DoubleMinAggregator.class);
    registerPersistentAggregator(SimplePageRankComputation.MAX_AGG, DoubleMaxAggregator.class);
  }
}