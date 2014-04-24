package stanford.infolab.debugger.testgenerator.scenario;

import java.util.Collection;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This interface encapsulates a unified test scenario interface expected by other modules of our
 * system.
 * 
 * The suppressed warning is necessary for old Hadoop version where most subclasses I of Writable
 * implements WritableComparable interface instead of WritableComparable<I> interface.
 * 
 * @author Brian Truong
 * 
 * @param <I> Vertex ID
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M1> Incoming messages
 * @param <M2> Outgoing messages
 */
@SuppressWarnings("rawtypes")
public interface IScenario<I extends WritableComparable, V extends Writable, E extends Writable, 
    M1 extends Writable, M2 extends Writable> {

  public I getVertexId();

  public void setVertexId(I vertexId);

  public V getVertexValue();

  public void setVertexValue(V vertexValue);

  public Collection<M1> getIncomingMessages();

  public void addIncomingMessage(M1 message);

  public Collection<I> getNeighbors();

  public void addNeighbor(I neighborId, E edgeValue);

  public E getEdgeValue(I neighborId);

  public void setEdgeValue(I neighborId, E edgeValue);

  public Collection<M2> getOutgoingMessages(I neighborId);

  public void addOutgoingMessage(I neighborId, M2 msg);
}
