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
package org.apache.giraph.debugger.examples.graphcoloring;

import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * DebugConfig for graph coloring for demonstration purposes.
 */
public class GraphColoringDebugConfig
  extends DebugConfig<LongWritable, VertexValue, NullWritable, Message, Message> {

  /**
   * @return list of vertices to capture, specified by ID.
   */
  @Override
  public List<LongWritable> verticesToCaptureByID() {
    ArrayList<LongWritable> idsToCapture = new ArrayList<LongWritable>();
    idsToCapture.add(new LongWritable(23));
    return idsToCapture;
  }
  
  @Override
  public int numberOfRandomVerticesToCapture() { return 5; }

  /**
   * @return whether neighbors of random or specified vertices should be captured.
   */
  @Override
  public boolean shouldCaptureNeighborsOfVertices() { return true; }

  @Override
  public boolean isVertexValueCorrect(LongWritable vertexId, VertexValue value) {
    return value.isColored() &&
      value.getState().equals(VertexValue.State.IN_SET);
  }

  @Override
  public boolean isMessageCorrect(LongWritable srcId, LongWritable dstId,
    Message message, long superstepNo) {
    return message.getType() != null && srcId.get() != dstId.get();
  }
}
