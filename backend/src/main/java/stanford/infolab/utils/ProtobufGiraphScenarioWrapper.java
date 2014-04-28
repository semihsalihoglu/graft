package utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.Computation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @param   <T> The type of the test scenarios.
 * @author  Brian Truong
 */
@SuppressWarnings("rawtypes")
public class ProtobufGiraphScenarioWrapper<T extends ProtobufScenario<? extends WritableComparable, ? extends Writable, 
    ? extends Writable, ? extends Writable, ? extends Writable>> {

  private Class<? extends Computation<?,?,?,?,?>> classUnderTest;
  private Class<? extends WritableComparable> vertexIdClass;
  private Class<? extends Writable> vertexValueClass;
  private Class<? extends Writable> edgeValueClass;
  private Class<? extends Writable> incomingMessageClass;
  private Class<? extends Writable> outgoingMessageClass;
  
  private ArrayList<T> testScenarios = new ArrayList<>();
  
  public ProtobufGiraphScenarioWrapper(Class<? extends Computation<?,?,?,?,?>> classUnderTest, 
      Class<? extends WritableComparable> vertexIdClass, 
      Class<? extends Writable> vertexValueClass, 
      Class<? extends Writable> edgeValueClass, 
      Class<? extends Writable> incomingMessageClass, 
      Class<? extends Writable> outgoingMessageClass) {
    this.classUnderTest = classUnderTest;
    this.vertexIdClass = vertexIdClass;
    this.vertexValueClass = vertexValueClass;
    this.edgeValueClass = edgeValueClass;
    this.incomingMessageClass = incomingMessageClass;
    this.outgoingMessageClass = outgoingMessageClass;
  }
  
  public Class<? extends Computation<?, ?, ?, ?, ?>> getClassUnderTest() {
    return classUnderTest;
  }
  
  public Class<? extends WritableComparable> getVertexIdClass() {
    return vertexIdClass;
  }

  public Class<? extends Writable> getVertexValueClass() {
    return vertexValueClass;
  }

  public Class<? extends Writable> getEdgeValueClass() {
    return edgeValueClass;
  }

  public Class<? extends Writable> getIncomingMessageClass() {
    return incomingMessageClass;
  }

  public Class<? extends Writable> getOutgoingMessageClass() {
    return outgoingMessageClass;
  }

  public List<T> getScenarios() {
    return testScenarios;
  }

  public void addScenario(T testScenario) {
    this.testScenarios.add(testScenario);
  }
}

@SuppressWarnings("rawtypes")
class ProtobufScenario<I extends WritableComparable, V extends Writable, E extends Writable, 
    M1 extends Writable, M2 extends Writable> {

  private I vertexId;
  private V vertexValue;
  private ArrayList<M1> inMsgs;
  private Map<I, Nbr> outNbrMap;

  public ProtobufScenario() {
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

  public I getVertexId() {
    return vertexId;
  }

  public void setVertexId(I vertexId) {
    this.vertexId = vertexId;
  }

  public V getVertexValue() {
    return vertexValue;
  }

  public void setVertexValue(V vertexValue) {
    this.vertexValue = vertexValue;
  }

  public Collection<M1> getIncomingMessages() {
    return inMsgs;
  }

  public void addIncomingMessage(M1 message) {
    inMsgs.add(message);
  }

  public Collection<I> getNeighbors() {
    return outNbrMap.keySet();
  }

  public void addNeighbor(I neighborId, E edgeValue) {
    if (outNbrMap.containsKey(neighborId)) {
      outNbrMap.get(neighborId).edgeValue = edgeValue;
    } else {
      outNbrMap.put(neighborId, new Nbr(edgeValue));
    }
  }

  public E getEdgeValue(I neighborId) {
    checkLoaded(outNbrMap);
    Nbr nbr = outNbrMap.get(neighborId);
    return nbr == null ? null : nbr.edgeValue;
  }

  public void setEdgeValue(I neighborId, E edgeValue) {
    if (outNbrMap.containsKey(neighborId)) {
      outNbrMap.get(neighborId).edgeValue = edgeValue;
    } else {
      outNbrMap.put(neighborId, new Nbr(edgeValue));
    }
  }

  public Collection<M2> getOutgoingMessages(I neighborId) {
    Nbr nbr = outNbrMap.get(neighborId);
    return nbr == null ? null : nbr.msgs;
  }

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
