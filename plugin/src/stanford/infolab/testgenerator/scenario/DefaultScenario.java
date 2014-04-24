package stanford.infolab.debugger.testgenerator.scenario;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A default implementation of {@link IScenario}. 
 * @author Brian Truong
 *
 * @param <I>   Vertex ID
 * @param <V>   Vertex value
 * @param <E>   Edge value
 * @param <M1>  Incoming message
 * @param <M2>  Outgoing message
 */
@SuppressWarnings("rawtypes")
public class DefaultScenario<I extends WritableComparable, V extends Writable, E extends Writable, 
    M1 extends Writable, M2 extends Writable> implements IScenario<I, V, E, M1, M2> {

  private I vertexId;
  private V vertexValue;
  private ArrayList<M1> inMsgs;
  private Map<I, Nbr> outNbrMap;

  public DefaultScenario() {
    reset();
  }
  
  void reset() {
    this.vertexId = null;
    this.vertexValue = null;
    this.inMsgs = new ArrayList<>();
    this.outNbrMap = new HashMap<>();
  }

  private void checkLoaded(Object arg) {
    if (arg == null) {
      throw new IllegalStateException("The ProtoBuf scenario has not been loaded or initialized.");
    }
  }

  @Override
  public I getVertexId() {
    return vertexId;
  }

  public void setVertexId(I vertexId) {
    this.vertexId = vertexId;
  }

  @Override
  public V getVertexValue() {
    return vertexValue;
  }

  @Override
  public void setVertexValue(V vertexValue) {
    this.vertexValue = vertexValue;
  }

  @Override
  public Collection<M1> getIncomingMessages() {
    return inMsgs;
  }

  public void addIncomingMessage(M1 message) {
    inMsgs.add(message);
  }

  @Override
  public Collection<I> getNeighbors() {
    return outNbrMap.keySet();
  }

  @Override
  public void addNeighbor(I neighborId, E edgeValue) {
    if (outNbrMap.containsKey(neighborId)) {
      outNbrMap.get(neighborId).edgeValue = edgeValue;
    } else {
      outNbrMap.put(neighborId, new Nbr(edgeValue));
    }
  }

  @Override
  public E getEdgeValue(I neighborId) {
    checkLoaded(outNbrMap);
    Nbr nbr = outNbrMap.get(neighborId);
    return nbr == null ? null : nbr.edgeValue;
  }

  @Override
  public void setEdgeValue(I neighborId, E edgeValue) {
    if (outNbrMap.containsKey(neighborId)) {
      outNbrMap.get(neighborId).edgeValue = edgeValue;
    } else {
      outNbrMap.put(neighborId, new Nbr(edgeValue));
    }
  }

  @Override
  public Collection<M2> getOutgoingMessages(I neighborId) {
    Nbr nbr = outNbrMap.get(neighborId);
    return nbr == null ? null : nbr.msgs;
  }

  @Override
  public void addOutgoingMessage(I neighborId, M2 msg) {
    if (!outNbrMap.containsKey(neighborId)) {
      outNbrMap.put(neighborId, new Nbr(null));
    }
    outNbrMap.get(neighborId).msgs.add(msg);
  }

  /**
   * A private neighbor object.
   * 
   * @author Brian Truong
   */
  private class Nbr {
    private E edgeValue;
    private ArrayList<M2> msgs;

    public Nbr(E edgeValue) {
      this(edgeValue, new ArrayList<M2>());
    }

    public Nbr(E edgeValue, ArrayList<M2> msgs) {
      this.edgeValue = edgeValue;
      this.msgs = msgs;
    }
  }
}
