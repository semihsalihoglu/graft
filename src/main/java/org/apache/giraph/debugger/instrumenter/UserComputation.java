package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A dummy Computation class that will sit between the
 * {@link AbstractInterceptingComputation} class at the top and the
 * {@link BottomInterceptingComputation} at the bottom.
 * 
 * @author netj
 */
@SuppressWarnings("rawtypes")
public abstract class UserComputation<I extends WritableComparable, V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
  extends AbstractInterceptingComputation<I, V, E, M1, M2> {

  @Override
  public void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    throw new NotImplementedException();
  }

}
