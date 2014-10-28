package org.apache.giraph.debugger.examples.randomwalk;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class RandomWalkVertexValueConstraintDebugConfig
  extends
  DebugConfig<LongWritable, IntWritable, NullWritable, IntWritable, IntWritable> {

  @Override
  public boolean shouldCatchExceptions() {
    return false;
  }

  @Override
  public boolean shouldDebugVertex(
    Vertex<LongWritable, IntWritable, NullWritable> vertex) {
    return false;
  }

  @Override
  public boolean shouldCheckVertexValueIntegrity() {
    return true;
  }

  @Override
  public boolean isVertexValueCorrect(LongWritable vertexId, IntWritable value) {
    return value.get() > 0;
  }

}
