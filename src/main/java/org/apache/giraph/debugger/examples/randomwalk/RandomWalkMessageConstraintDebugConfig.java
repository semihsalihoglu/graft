package org.apache.giraph.debugger.examples.randomwalk;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class RandomWalkMessageConstraintDebugConfig
  extends
  DebugConfig<LongWritable, IntWritable, NullWritable, IntWritable, IntWritable> {

  @Override
  public boolean shouldCatchExceptions() {
    return false;
  }

  @Override
  public boolean shouldDebugVertex(
    Vertex<LongWritable, IntWritable, NullWritable> vertex, long superstepNo) {
    return false;
  }
  
  @Override
  public boolean shouldCheckMessageIntegrity() {
    return true;
  }

  @Override
  public boolean isMessageCorrect(LongWritable srcId, LongWritable dstId,
    IntWritable message, long superstepNo) {
    return message.get() > 0;
  }

}
