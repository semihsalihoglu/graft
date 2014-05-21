package stanford.infolab.debugger.examples.integrity;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import stanford.infolab.debugger.instrumenter.AbstractInterceptingComputation;

/**
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id
 * in the component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest
 * vertex id along the edges to all vertices of a connected component. The
 * number of supersteps necessary is equal to the length of the maximum
 * diameter of all components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
public abstract class ConnectedComponentsDebugComputationModified extends
  AbstractInterceptingComputation<IntWritable, IntWritable, NullWritable, IntWritable, IntWritable> {

  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param vertex Vertex
   * @param messages Iterator of messages from the previous superstep.
   * @throws IOException
   */
  public void compute(
      Vertex<IntWritable, IntWritable, NullWritable> vertex,
      Iterable<IntWritable> messages) throws IOException {
    int currentComponent = vertex.getValue().get();

    if (getSuperstep() == 0) {
      vertex.setValue(new IntWritable(currentComponent));
      for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());
      }
      vertex.voteToHalt();
      return;
    }

    boolean changed = false;
    // did we get a smaller id ?
    for (IntWritable message : messages) {
      int candidateComponent = message.get();
      // INTENTIONAL BUG: in the original algorithm the value of the comparison sign should be <.
      if (candidateComponent > currentComponent) {
        System.out.println("changing value in superstep: " + getSuperstep() + " vertex.id: "
          + vertex.getId() +  " newComponent: "+ candidateComponent);
        currentComponent = candidateComponent;
        changed = true;
      }
    }

    // propagate new component id to the neighbors
    if (changed) {
      vertex.setValue(new IntWritable(currentComponent));
      for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());        
      }
    }
    vertex.voteToHalt();
  }

}
