package org.apache.giraph.debugger.examples.exceptiondebug;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Debug configuration file for SimpleTriangleClosingDebugConfig, that is configured to catch
 * exceptions.
 * 
 * @author semihsalihoglu
 */
public class SimpleTriangleClosingDebugConfig extends DebugConfig<IntWritable,
  IntWritable, NullWritable, IntWritable, IntWritable>{
  @Override
  public boolean shouldCatchExceptions() {
    return true;
  }
}
