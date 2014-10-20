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

import java.io.IOException;

import org.apache.giraph.debugger.examples.graphcoloring.GraphColoringMaster.Phase;
import org.apache.giraph.debugger.examples.graphcoloring.VertexValue.State;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class GraphColoringComputation extends
  BasicComputation<LongWritable, VertexValue, NullWritable, Message> {

  private static final LongWritable ONE = new LongWritable(1);
  private Phase phase;
  private int colorToAssign;

  @Override
  public void preSuperstep() {
    phase = Phase.values()[((IntWritable) getAggregatedValue(GraphColoringMaster.PHASE))
      .get()];
    colorToAssign = ((IntWritable) getAggregatedValue(GraphColoringMaster.COLOR_TO_ASSIGN))
      .get();
  }

  @Override
  public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex,
    Iterable<Message> messages) throws IOException {
    // Treat already colored vertices as if it didn't exist in the graph.
    if (vertex.getValue().isColored()) {
      vertex.voteToHalt();
      return;
    }

    State state = vertex.getValue().getState();
    // Nothing's left to do if this vertex has been placed in an independent set
    // already.
    if (state == State.IN_SET) {
      return;
    }

    switch (phase) {
    case LOTTERY:
      switch (state) {
      case UNKNOWN:
        // Unknown vertices will go through a lottery, and be put in
        // "potentially in set" state with probability 1/2d where d is its
        // degree.
        if (Math.random() * 2 * vertex.getNumEdges() < 1) {
          vertex.getValue().setState(State.TENTATIVELY_IN_SET);
          sendMessageToAllEdges(vertex, new Message(vertex,
            Message.Type.WANTS_TO_BE_IN_SET));
        }
        break;

      default:
        // Nothing to do for others.
        break;
      }
      break;

    case CONFLICT_RESOLUTION:
      switch (state) {
      case TENTATIVELY_IN_SET:
        // When a vertex potentially in set receives a message from its
        // neighbor, it must resolve conflicts by deciding to put the vertex
        // that has the minimum vertex id.
        long myId = vertex.getId().get();
        long minId = myId;
        for (Message message : messages) {
          assert message.getType() == Message.Type.WANTS_TO_BE_IN_SET;
          long neighborId = message.getSenderVertex();
          if (neighborId < minId) {
            minId = neighborId;
          }
        }
        if (minId == myId) {
          // Put this vertex in the independent set if it has the minimum id.
          vertex.getValue().setState(State.IN_SET);
          sendMessageToAllEdges(vertex, new Message(vertex,
            Message.Type.IS_IN_SET));
        } else {
          // Otherwise, it's unknown whether this vertex will be in the final
          // independent set.
        }
        break;

      default:
        // Nothing to do for others.
        break;
      }

    case EDGE_CLEANING:
      // Count the number of messages received.
      int numNeighborsMovedIntoSet = 0;
      for (Message message : messages) {
        assert message.getType() == Message.Type.IS_IN_SET;
        vertex.removeEdges(new LongWritable(message.getSenderVertex()));
        ++numNeighborsMovedIntoSet;
      }
      if (numNeighborsMovedIntoSet > 0) {
        // At this phase, we know any vertex that received a notification from
        // its neighbor cannot belong to the set.
        vertex.getValue().setState(State.NOT_IN_SET);
      } else {
        // Otherwise, we put the vertex back into unknown state, so they can go
        // through another lottery.
        vertex.getValue().setState(State.UNKNOWN);
        aggregate(GraphColoringMaster.NUM_VERTICES_UNKNOWN, ONE);
        // XXX INTENTIONAL BUG: NOT_IN_SET vertices that did not receive any
        // IS_IN_SET message will also go back to UNKNOWN state, which is
        // undesired.
      }
      break;

    case COLOR_ASSIGNMENT:
      if (state == State.IN_SET) {
        // Assign current cycle's color to all IN_SET vertices.
        vertex.getValue().setColor(colorToAssign);
        // Aggregate number of colored vertices.
        aggregate(GraphColoringMaster.NUM_VERTICES_COLORED, ONE);
      } else {
        // For all other vertices, move their state back to UNKNOWN, so they can
        // go through another round of maximal independent set finding.
        vertex.getValue().setState(State.UNKNOWN);
      }
      break;

    default:
      throw new IllegalStateException();
    }
  }

}
