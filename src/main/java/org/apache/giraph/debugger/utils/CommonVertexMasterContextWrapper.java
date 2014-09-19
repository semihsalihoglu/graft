/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.debugger.GiraphAggregator.AggregatedValue;
import org.apache.giraph.debugger.Scenario.CommonVertexMasterContext;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around
 * {@link org.apache.giraph.debugger.Scenario.CommonVertexMasterContext}
 * protocol buffer.
 * 
 * @author semihsalihoglu
 */
@SuppressWarnings("rawtypes")
public class CommonVertexMasterContextWrapper extends BaseWrapper {
  private ImmutableClassesGiraphConfiguration immutableClassesConfig = null;
  private long superstepNo;
  private long totalNumVertices;
  private long totalNumEdges;
  private ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers;

  public CommonVertexMasterContextWrapper() {
    this.superstepNo = -1;
    this.totalNumVertices = -1;
    this.totalNumEdges = -1;
    this.previousAggregatedValueWrappers = new ArrayList<>();
  }

  public CommonVertexMasterContextWrapper(
    ImmutableClassesGiraphConfiguration immutableClassesConfig,
    long superstepNo, long totalNumVertices, long totalNumEdges) {
    this.immutableClassesConfig = immutableClassesConfig;
    this.superstepNo = superstepNo;
    this.totalNumVertices = totalNumVertices;
    this.totalNumEdges = totalNumEdges;
  }

  public long getSuperstepNoWrapper() {
    return superstepNo;
  }

  public long getTotalNumVerticesWrapper() {
    return totalNumVertices;
  }

  public long getTotalNumEdgesWrapper() {
    return totalNumEdges;
  }

  public void setSuperstepNoWrapper(long superstepNo) {
    this.superstepNo = superstepNo;
  }

  public void setTotalNumVerticesWrapper(long totalNumVertices) {
    this.totalNumVertices = totalNumVertices;
  }

  public void setTotalNumEdgesWrapper(long totalNumEdges) {
    this.totalNumEdges = totalNumEdges;
  }

  public void addPreviousAggregatedValue(
    AggregatedValueWrapper previousAggregatedValueWrapper) {
    this.previousAggregatedValueWrappers.add(previousAggregatedValueWrapper);
  }

  public void setPreviousAggregatedValues(
    ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers) {
    this.previousAggregatedValueWrappers = previousAggregatedValueWrappers;
  }

  public Collection<AggregatedValueWrapper> getPreviousAggregatedValues() {
    return previousAggregatedValueWrappers;
  }

  public ImmutableClassesGiraphConfiguration getConfig() {
    return immutableClassesConfig;
  }

  public void setConfig(
    ImmutableClassesGiraphConfiguration immutableClassesConfig) {
    this.immutableClassesConfig = immutableClassesConfig;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    CommonVertexMasterContext.Builder commonContextBuilder = CommonVertexMasterContext
      .newBuilder();
    commonContextBuilder.setConf(toByteString(immutableClassesConfig))
      .setSuperstepNo(getSuperstepNoWrapper())
      .setTotalNumVertices(getTotalNumVerticesWrapper())
      .setTotalNumEdges(getTotalNumEdgesWrapper());

    for (AggregatedValueWrapper aggregatedValueWrapper : getPreviousAggregatedValues()) {
      commonContextBuilder
        .addPreviousAggregatedValue((AggregatedValue) aggregatedValueWrapper
          .buildProtoObject());
    }
    return commonContextBuilder.build();
  }

  @Override
  public void loadFromProto(GeneratedMessage generatedMessage)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    CommonVertexMasterContext commonContext = (CommonVertexMasterContext) generatedMessage;
    GiraphConfiguration config = new GiraphConfiguration();
    fromByteString(commonContext.getConf(), config);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = new ImmutableClassesGiraphConfiguration(
      config);
    setConfig(immutableClassesGiraphConfiguration);

    setSuperstepNoWrapper(commonContext.getSuperstepNo());
    setTotalNumVerticesWrapper(commonContext.getTotalNumVertices());
    setTotalNumEdgesWrapper(commonContext.getTotalNumEdges());

    for (AggregatedValue previousAggregatedValueProto : commonContext
      .getPreviousAggregatedValueList()) {
      AggregatedValueWrapper aggregatedValueWrapper = new AggregatedValueWrapper();
      aggregatedValueWrapper.loadFromProto(previousAggregatedValueProto);
      addPreviousAggregatedValue(aggregatedValueWrapper);
    }
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return CommonVertexMasterContext.parseFrom(inputStream);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("\nconfig: " + immutableClassesConfig.toString());
    stringBuilder.append("superstepNo: " + getSuperstepNoWrapper());
    stringBuilder.append("\ntotalNumVertices: " + totalNumVertices);
    stringBuilder.append("\ntotalNumEdges: " + totalNumEdges);
    stringBuilder.append("\nnumAggregators: " +
      getPreviousAggregatedValues().size());
    for (AggregatedValueWrapper aggregatedValueWrapper : getPreviousAggregatedValues()) {
      stringBuilder.append("\n" + aggregatedValueWrapper);
    }
    return stringBuilder.toString();
  }
}
