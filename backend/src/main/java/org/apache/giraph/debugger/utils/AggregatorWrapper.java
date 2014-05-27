package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import stanford.infolab.debugger.GiraphAggregator.AggregatedValue;
import stanford.infolab.debugger.GiraphAggregator.Aggregator.Builder;

import com.google.protobuf.GeneratedMessage;


/**
 * Wrapper class around {@link org.apache.giraph.debugger.GiraphAggregator.Aggregator}
 * protocol buffer.
 * 
 * @author semihsalihoglu
 */
public class AggregatorWrapper extends BaseWrapper {

  private String key;
  private Aggregator<Writable> aggregator;

  public AggregatorWrapper(String key, Aggregator aggregator) {
    this.key = key;
    this.aggregator = aggregator;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    Builder aggregatorProtoBuilder = stanford.infolab.debugger.GiraphAggregator.Aggregator.newBuilder();
    aggregatorProtoBuilder.setAggregatorClass(aggregator.getClass().getName());
    aggregatorProtoBuilder.setAggregatedValue((AggregatedValue)
      new AggregatedValueWrapper(key, aggregator.getAggregatedValue()).buildProtoObject());
    return aggregatorProtoBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return stanford.infolab.debugger.GiraphAggregator.Aggregator.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage protoObject) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    stanford.infolab.debugger.GiraphAggregator.Aggregator aggregatorProto = 
      (stanford.infolab.debugger.GiraphAggregator.Aggregator) protoObject;
    Aggregator<Writable> giraphAggregator = 
      (org.apache.giraph.aggregators.Aggregator<Writable>)
      Class.forName(aggregatorProto.getAggregatorClass()).newInstance();
    AggregatedValue aggregatedValueProto = aggregatorProto.getAggregatedValue();
    this.key = aggregatedValueProto.getKey();
    Writable giraphAggregatedValue = (Writable) Class.forName(
      aggregatedValueProto.getWritableClass()).newInstance();
    WritableUtils.readFieldsFromByteArray(aggregatedValueProto.getValue().toByteArray(),
      giraphAggregatedValue);
    giraphAggregator.setAggregatedValue(giraphAggregatedValue);
  }

  public String getKey() {
    return key;
  }

  public Aggregator<Writable> getAggregator() {
    return aggregator;
  }
  
  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("\nkey: " + key);
    stringBuilder.append(" aggregatorClass: " + aggregator.getClass().getName());
    stringBuilder.append(" aggregatedValueClass: " + aggregator.getAggregatedValue().getClass().getName());
    stringBuilder.append(" value: " + aggregator.getAggregatedValue());
    return stringBuilder.toString();
  }
}
