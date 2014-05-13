package stanford.infolab.debugger.instrumenter;

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
public abstract class DebugConfig<I extends WritableComparable, V extends Writable,
  E extends Writable, M1 extends Writable, M2 extends Writable> {

  public boolean shouldDebugSuperstep(long superstepNo) {
    return false;
  }
  
  public boolean shouldDebugVertex(I vertexId) {
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
