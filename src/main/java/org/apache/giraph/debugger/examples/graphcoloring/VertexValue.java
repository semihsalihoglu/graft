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

  /**
   * Value for an invalid color.
   */
  public static final String NO_COLOR = "--";

  /**
   * Color of the vertex.
   */
  private String color = NO_COLOR;

  /**
   * State of the vertex.
   */
  public enum State {
    /**
     * Unknown state.
     */
    UNKNOWN("U"),
    /**
     * State of tentatively in the independent set.
     */
    TENTATIVELY_IN_SET("T"),
    /**
     * State of not in the independent set.
     */
    NOT_IN_SET("N"),
    /**
     * State of being in the independent set.
     */
    IN_SET("I");

    /**
     * Abbreviation string of the state.
     */
    private final String abbreviation;

    /**
     * Constructor with abbreviation string.
     * @param abbreviation shorthand string for the state.
     */
    private State(String abbreviation) {
      this.abbreviation = abbreviation;
    }

    public String getAbbreviation() {
      return abbreviation;
    }

  }

  /**
   * State of the vertex.
   */
  private State state = State.UNKNOWN;

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public void setColor(String color) {
    this.color = color;
  }

  public boolean isColored() {
    return !NO_COLOR.equals(color);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    state = State.values()[in.readInt()];
    color = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(state.ordinal());
    out.writeUTF(color);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (NO_COLOR.equals(color)) {
      sb.append("MIS: ");
      switch (state) {
      case UNKNOWN:
      sb.append("U");
//        sb.append("?");
        break;
      case IN_SET:
      sb.append("I");
//        sb.append("\u2713");
        break;
      case TENTATIVELY_IN_SET:
      sb.append("T");
//        sb.append("\u2713" + "?");
        break;
      case NOT_IN_SET:
        sb.append("X");
        break;
      default:
        break;
      }
    } else {
      sb.append("COLOR: " + color);
    }
//    sb.append("col: " + color + " mis: " + state.abbreviation);
    return sb.toString();
  }

}
