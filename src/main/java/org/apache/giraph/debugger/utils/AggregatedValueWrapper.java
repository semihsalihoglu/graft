package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.debugger.GiraphAggregator.AggregatedValue;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around
 * {@link org.apache.giraph.debugger.GiraphAggregator.AggregatedValue} protocol
 * buffer.
 * 
 * @author semihsalihoglu
 */
public class AggregatedValueWrapper extends BaseWrapper {
  private String key;
  private Writable value;

  public AggregatedValueWrapper() {
  }

  public AggregatedValueWrapper(String key, Writable value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    AggregatedValue.Builder aggregatedValueBuilder =
      AggregatedValue.newBuilder();
    aggregatedValueBuilder.setWritableClass(value.getClass().getName());
    aggregatedValueBuilder.setKey(key);
    aggregatedValueBuilder.setValue(ByteString.copyFrom(WritableUtils
      .writeToByteArray(value)));
    return aggregatedValueBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return AggregatedValue.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage protoObject)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    AggregatedValue aggregatedValueProto = (AggregatedValue) protoObject;
    this.value =
      (Writable) Class.forName(aggregatedValueProto.getWritableClass())
        .newInstance();
    WritableUtils.readFieldsFromByteArray(aggregatedValueProto.getValue()
      .toByteArray(), this.value);
    this.key = aggregatedValueProto.getKey();
  }

  public String getKey() {
    return key;
  }

  public Writable getValue() {
    return value;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("\nkey: " + key);
    stringBuilder
      .append(" aggregatedValueClass: " + value.getClass().getName());
    stringBuilder.append(" value: " + value);
    return stringBuilder.toString();
  }
}
