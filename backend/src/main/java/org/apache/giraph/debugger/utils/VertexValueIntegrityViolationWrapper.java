package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.giraph.debugger.Integrity.VertexValueIntegrityViolation;
import org.apache.giraph.debugger.Integrity.VertexValueIntegrityViolation.VertexIdValuePair;

import com.google.protobuf.GeneratedMessage;

/**
 * A wrapper class around the contents of VertexValueIntegrityViolation inside integrity.proto. In
 * scenario.proto most things are stored as serialized byte arrays and this class gives
 * them access through the java classes that those byte arrays serialize.
 * 
 * @param <I> vertex ID class.
 * @param <V> vertex value class.
 * @author Semih Salihoglu
 */
@SuppressWarnings("rawtypes")
public class VertexValueIntegrityViolationWrapper<I extends WritableComparable, V extends Writable> 
  extends BaseScenarioAndIntegrityWrapper<I>{

  private Class<V> vertexValueClass;
  private List<VertexIdValuePairWrapper> vertexIdValuePairWrappers = new ArrayList<>();
  private long superstepNo;

  // Empty constructor to be used for loading from HDFS.
  public VertexValueIntegrityViolationWrapper() {}
  
  public VertexValueIntegrityViolationWrapper(Class<I> vertexIdClass,Class<V> vertexValueClass) {
    initialize(vertexIdClass, vertexValueClass);
  }

  private void initialize(Class<I> vertexIdClass, Class<V> vertexValueClass) {
    super.initialize(vertexIdClass);
    this.vertexValueClass = vertexValueClass;    
  }

  public Collection<VertexIdValuePairWrapper> getVertexIdValuePairWrappers() {
    return vertexIdValuePairWrappers;
  }

  public void addVertexIdPairWrapper(I vertexId, V vertexValue) {
    vertexIdValuePairWrappers.add(new VertexIdValuePairWrapper(makeCloneOf(vertexId, vertexIdClass),
      makeCloneOf(vertexValue, vertexValueClass)));
  }

  public int numVerteIdValuePairWrappers() {
    return vertexIdValuePairWrappers.size();
  }
  
  public Class<V> getVertexValueClass() {
    return vertexValueClass;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\nvertexValueClass: " + getVertexValueClass().getCanonicalName());
    for (VertexIdValuePairWrapper vertexIdValuePairWrapper
      : getVertexIdValuePairWrappers()) {
      stringBuilder.append("\n" + vertexIdValuePairWrapper);
    }
    return stringBuilder.toString();
  }
  
  public class VertexIdValuePairWrapper extends BaseWrapper {
    public I vertexId;
    public V vertexValue;

    public VertexIdValuePairWrapper(I vertexId, V vertexValue) {
      this.vertexId = vertexId;
      this.vertexValue = vertexValue;
    }

    public VertexIdValuePairWrapper() {
      // TODO Auto-generated constructor stub
    }

    @Override
    public String toString() {
      return "VertexIdValuePairWrapper: srcId: " + vertexId + " value: " + vertexValue; 
    }

    @Override
    public GeneratedMessage buildProtoObject() {
      VertexIdValuePair.Builder vertexIdValuePairBuilder =
        VertexIdValuePair.newBuilder();
      vertexIdValuePairBuilder.setVertexId(toByteString(this.vertexId));
      vertexIdValuePairBuilder.setVertexValue(toByteString(this.vertexValue));
      return vertexIdValuePairBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
      return VertexIdValuePair.parseFrom(inputStream);
    }

    @Override
    public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
      IOException, InstantiationException, IllegalAccessException {
      VertexIdValuePair vertexIdValuePair = (VertexIdValuePair) generatedMessage;
      this.vertexId = newInstance(vertexIdClass);
      fromByteString(vertexIdValuePair.getVertexId(), vertexId);
      this.vertexValue = newInstance(vertexValueClass);
      fromByteString(vertexIdValuePair.getVertexValue(), vertexValue);
    }
  }

  public long getSuperstepNo() {
    return superstepNo;
  }

  public void setSuperstepNo(long superstepNo) {
    this.superstepNo = superstepNo;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    VertexValueIntegrityViolation.Builder vertexValueIntegrityViolationBuilder =
      VertexValueIntegrityViolation.newBuilder();
    vertexValueIntegrityViolationBuilder.setVertexIdClass(getVertexIdClass().getName());
    vertexValueIntegrityViolationBuilder.setVertexValueClass(getVertexValueClass().getName());
    vertexValueIntegrityViolationBuilder.setSuperstepNo(getSuperstepNo());
    for (VertexIdValuePairWrapper vertexIdValuePairWrapper : vertexIdValuePairWrappers) {
      vertexValueIntegrityViolationBuilder.addVertexIdValuePair(
        (VertexIdValuePair) vertexIdValuePairWrapper.buildProtoObject());
    }
    return vertexValueIntegrityViolationBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return VertexValueIntegrityViolation.parseFrom(inputStream);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void loadFromProto(GeneratedMessage protoObject) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    VertexValueIntegrityViolation vertexValueIntegrityViolation =
      (VertexValueIntegrityViolation) protoObject;
    
    Class<I> vertexIdClass = (Class<I>) castClassToUpperBound(
      Class.forName(vertexValueIntegrityViolation.getVertexIdClass()), WritableComparable.class);

    Class<V> vertexValueClass = (Class<V>) castClassToUpperBound(
      Class.forName(vertexValueIntegrityViolation.getVertexValueClass()), Writable.class);

    initialize(vertexIdClass, vertexValueClass);
    setSuperstepNo(vertexValueIntegrityViolation.getSuperstepNo());

    for (VertexIdValuePair vertexIdValuePair
      : vertexValueIntegrityViolation.getVertexIdValuePairList()) {
      VertexIdValuePairWrapper vertexIdValuePairWrapper = new VertexIdValuePairWrapper();
      vertexIdValuePairWrapper.loadFromProto(vertexIdValuePair);
      vertexIdValuePairWrappers.add(vertexIdValuePairWrapper);
    }
  }
}
