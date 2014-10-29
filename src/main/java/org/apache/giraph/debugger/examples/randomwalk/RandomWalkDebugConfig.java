package org.apache.giraph.debugger.examples.randomwalk;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class RandomWalkDebugConfig
  extends
  DebugConfig<LongWritable, IntWritable, NullWritable, IntWritable, IntWritable> {

  @Override
  public boolean shouldCheckVertexValueIntegrity() {
    return true;
  }

  @Override
  public boolean isVertexValueCorrect(LongWritable vertexId, IntWritable value) {
    return value.get() > 0;
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
