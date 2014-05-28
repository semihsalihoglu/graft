package org.apache.giraph.debugger.instrumenter;

import org.apache.commons.lang.NotImplementedException;

/**
 * A dummy MasterCompute class that will sit between the
 * {@link AbstractInterceptingMasterCompute} class at the top and the
 * {@link BottomInterceptingMasterCompute} at the bottom.
 * 
 * @author netj
 */
public abstract class UserMasterCompute extends AbstractInterceptingMasterCompute {

  @Override
  public void compute() {
    throw new NotImplementedException();
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    throw new NotImplementedException();
  }
}
