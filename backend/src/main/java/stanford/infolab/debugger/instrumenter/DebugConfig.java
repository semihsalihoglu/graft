package stanford.infolab.debugger.instrumenter;

import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** 
 * This class is used by programmers to configure what they want to be debugged. Programmers
 * can specify which vertices to debug, etc... TODO(semih): Fill this as more features are added
 * 
 * @author semihsalihoglu
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
@SuppressWarnings({ "rawtypes" })
public abstract class DebugConfig<I extends WritableComparable, V extends Writable,
  E extends Writable, M1 extends Writable, M2 extends Writable> {
  private Set<I> verticesToDebugSet;
  private boolean debugNeighborsOfVerticesToDebug;
  private boolean debugAllSupersteps;
  
  public DebugConfig() {
    verticesToDebugSet = null;
    debugNeighborsOfVerticesToDebug = false;
    debugAllSupersteps = false;
  }
  
  public DebugConfig(boolean debugNeighborsOfVerticesToDebug, I...verticesToDebug) {
    this.debugNeighborsOfVerticesToDebug = debugNeighborsOfVerticesToDebug;
    this.verticesToDebugSet = new HashSet<>();
    this.verticesToDebugSet.addAll(verticesToDebugSet);
    this.debugAllSupersteps = true;
  }

  public boolean shouldDebugSuperstep(long superstepNo) {
    return debugAllSupersteps;
  }
  
  public boolean shouldDebugVertex(Vertex<I, V, E> vertex) {
    if (verticesToDebugSet == null) {
      return false;     
    } else {
      return verticesToDebugSet.contains(vertex.getId()) || (debugAllSupersteps &&
        isVertexANeighborOfAVertexToDebug(vertex));
    }
  }
  
  private boolean isVertexANeighborOfAVertexToDebug(Vertex<I, V, E> vertex) {
    for (Edge<I, E> edge : vertex.getEdges()) {
      if (verticesToDebugSet.contains(edge.getTargetVertexId())) {
        return true;
      }
    }
    return false;
  }

  public boolean shouldCatchExceptions() {
    return false;
  }
  
  public boolean shouldCheckMessageIntegrity() {
    return false;
  }
  
  public boolean isMessageCorrect(I srcId, I dstId, M1 message) {
    return true;
  }

  public boolean shouldCheckVertexValueIntegrity() {
    return false;
  }
  
  public boolean isVertexValueCorrect(I vertexId, V value) {
    return true;
  }
}
