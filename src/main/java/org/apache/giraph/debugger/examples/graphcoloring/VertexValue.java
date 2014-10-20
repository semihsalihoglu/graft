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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Vertex value for maximal independent set computation.
 */
public class VertexValue implements Writable {

  public final static int NO_COLOR = -1;

  private int color = NO_COLOR;

  public enum State {
    UNKNOWN, TENTATIVELY_IN_SET, NOT_IN_SET, IN_SET,
  }

  private State state = State.UNKNOWN;

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public void setColor(int color) {
    this.color = color;
  }
  
  public boolean isColored() {
    return state == State.IN_SET && color != NO_COLOR;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    state = State.values()[in.readInt()];
    color = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(state.ordinal());
    out.writeInt(color);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(color == NO_COLOR ? "?" : color);
    sb.append(":");
    sb.append(state);
    return sb.toString();
  }

}
