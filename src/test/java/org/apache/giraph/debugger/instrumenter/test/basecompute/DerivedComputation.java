package org.apache.giraph.debugger.instrumenter.test.basecompute;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class DerivedComputation extends BaseComputation {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BasicComputation.class);

  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID = new LongConfOption(
    "SimpleShortestPathsVertex.sourceId", 1, "The shortest paths id");

  private double minDist;

  @Override
  protected void collect(
    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
    Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }
    if (getSuperstep() == 8)
      throw new RuntimeException("bug");
    minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
        " vertex value = " + vertex.getValue());
    }
  }

  @Override
  protected void signal(
    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
    Iterable<DoubleWritable> messages) {
    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
            edge.getTargetVertexId() + " = " + distance);
        }
        // INTENTIONAL BUG:Instead of sending the distance (i.e. by
        // adding edge values), we send the vertex value.
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(minDist));
      }
    }
  }

  /**
   * Is this vertex the source id?
   * 
   * @param vertex
   *          Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }
}
