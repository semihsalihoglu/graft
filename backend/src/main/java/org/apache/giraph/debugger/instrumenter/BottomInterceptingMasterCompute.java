package org.apache.giraph.debugger.instrumenter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BottomInterceptingMasterCompute extends UserMasterCompute {

  @Intercept
  @Override
  public void compute() {
    interceptComputeBegin();
    try {
      super.compute();
      interceptComputeEnd();
    } catch (Exception e) {
      interceptComputeException(e);
      throw e;
    }
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {}

  @Override
  public void write(DataOutput arg0) throws IOException {}
}