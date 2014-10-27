package org.apache.giraph.debugger.examples.pagerank;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

public class SimplePageRankComputation extends
  BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {

  /** Number of supersteps for this test */
  public static int MAX_SUPERSTEPS = 10;
  /** Logger */
  private static final Logger LOG = Logger
    .getLogger(SimplePageRankComputation.class);
  /** Sum aggregator name */
  static String SUM_AGG = "sum";
  /** Min aggregator name */
  static String MIN_AGG = "min";
  /** Max aggregator name */
  static String MAX_AGG = "max";

  @Override
  public void initialize(
    GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, DoubleWritable, NullWritable> workerClientRequestProcessor,
    GraphTaskManager<LongWritable, DoubleWritable, NullWritable> graphTaskManager,
    WorkerAggregatorUsage workerAggregatorUsage, WorkerContext workerContext) {
    MAX_SUPERSTEPS = workerContext.getConf().getInt(
      getClass().getName() + ".maxSupersteps", 10);
    super.initialize(graphState, workerClientRequestProcessor,
      graphTaskManager, workerAggregatorUsage, workerContext);
  }

  @Override
  public void compute(
    Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
    Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue = new DoubleWritable(
        (0.15f / getTotalNumVertices()) + 0.85f * sum);
      vertex.setValue(vertexValue);
      aggregate(MAX_AGG, vertexValue);
      aggregate(MIN_AGG, vertexValue);
      aggregate(SUM_AGG, new LongWritable(1));
      // LOG.info(vertex.getId() + ": PageRank=" + vertexValue + " max=" +
      // getAggregatedValue(MAX_AGG) + " min=" + getAggregatedValue(MIN_AGG));
    }

    if (getSuperstep() < MAX_SUPERSTEPS) {
      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() /
        edges));
    } else {
      vertex.voteToHalt();
    }
  }

}
