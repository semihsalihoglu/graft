package org.apache.giraph.debugger.instrumenter.test.basecompute;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class CommonDebugConfig
  extends
  DebugConfig<LongWritable, DoubleWritable, FloatWritable, DoubleWritable, DoubleWritable> {

  @Override
  public boolean shouldCatchExceptions() {
    return true;
  }

}
