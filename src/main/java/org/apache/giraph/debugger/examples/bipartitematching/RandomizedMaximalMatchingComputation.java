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
package org.apache.giraph.debugger.examples.bipartitematching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.debugger.examples.bipartitematching.RandomizedMaximalMatchingComputation.Message;
import org.apache.giraph.debugger.examples.bipartitematching.RandomizedMaximalMatchingComputation.VertexValue;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Randomized maximal bipartite graph matching algorithm implementation. It
 * assumes all vertices whose ids are even are in the left part, and odd in the
 * right.
 */
public class RandomizedMaximalMatchingComputation extends
  BasicComputation<LongWritable, VertexValue, NullWritable, Message> {

  @Override
  public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex,
    Iterable<Message> messages) throws IOException {

    int phase = (int) (getSuperstep() % 4);
    switch (phase) {
    case 0: // "In phase 0 of a cycle,"
      // "each left vertex not yet matched"
      if (isUnmatchedLeft(vertex)) {
        // "sends a message to each of its neighbors to request a match,"
        sendMessageToAllEdges(vertex, createRequestMessage(vertex));
        // "and then unconditionally votes to halt."
        vertex.voteToHalt();
      }
      // "If it sent no messages (because it is already matched, or has no
      // outgoing edges), or if all the message recipients are already
      // matched, it will never be reactivated. Otherwise, it will receive a
      // response in two supersteps and reactivate."
      break;

    case 1: // "In phase 1 of a cycle,"
      // "each right vertex not yet matched"
      if (isUnmatchedRight(vertex)) {
        int i = 0;
        for (Message msg : messages) {
          // "randomly chooses one of the messages it receives,"
          Message reply = (i == 0) ? // (by simply granting the first one)
            // "sends a message granting that request, and"
            createGrantingMessage(vertex) :
            // "sends messages to other requestors denying it."
            createDenyingMessage(vertex);
          sendMessage(msg.getSenderVertex(), reply);
          ++i;
        }
        // "Then it unconditionally votes to halt."
        vertex.voteToHalt();
      }
      break;

    case 2: // "In phase 2 of a cycle,"
      // "each left vertex not yet matched"
      if (isUnmatchedLeft(vertex)) {
        // "chooses one of the grants it receives"
        for (Message msg : messages) {
          if (msg.isGranting().get()) {
            // (by simply picking the first one)
            // "and sends an acceptance message."
            sendMessage(msg.getSenderVertex(), createGrantingMessage(vertex));
            // (and also record which vertex was matched)
            vertex.getValue().setMatchedVertex(msg.getSenderVertex());
            break;
          }
        }
        // "Left vertices that are already matched will never execute this
        // phase, since they will not have sent a message in phase 0."
      }
      break;

    case 3: // "Finally, in phase 3,"
      // "an unmatched right vertex"
      if (isUnmatchedRight(vertex)) {
        // "receives at most one acceptance message."
        for (Message msg : messages) {
          // "It notes the matched node"
          vertex.getValue().setMatchedVertex(msg.getSenderVertex());
          break;
        }
        // "and unconditionally votes to halt"
        vertex.voteToHalt();
        // "it has nothing further to do."
      }
      break;

    default:
      throw new IllegalStateException("No such phase " + phase);
    }
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex belongs to the left part
   */
  boolean isLeft(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return vertex.getId().get() % 2 == 1;
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex has a match
   */
  private boolean hasNotMatchedYet(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return vertex.getValue().matchedVertex == null;
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex is an unmatched left one
   */
  protected boolean isUnmatchedLeft(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return isLeft(vertex) && hasNotMatchedYet(vertex);
  }

  /**
   * @param vertex
   *          The vertex to test
   * @return Whether the vertex is an unmatched right one
   */
  protected boolean isUnmatchedRight(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return !isLeft(vertex) && hasNotMatchedYet(vertex);
  }

  /**
   * @param vertex
   *          Sending vertex
   * @return A message requesting a match
   */
  private Message createRequestMessage(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return new Message(vertex);
  }

  /**
   * @param vertex
   *          Sending vertex
   * @return A message granting the match request
   */
  private Message createGrantingMessage(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return new Message(vertex, true);
  }

  /**
   * @param vertex
   *          Sending vertex
   * @return A message denying the match request
   */
  private Message createDenyingMessage(
    Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    return new Message(vertex, false);
  }

  /**
   * Vertex value for bipartite matching.
   */
  public static class VertexValue implements Writable {

    /**
     * The id of the matching vertex on the other side.
     */
    private LongWritable matchedVertex;

    public LongWritable getMatchedVertex() {
      return matchedVertex;
    }

    public void setMatchedVertex(LongWritable matchedVertex) {
      this.matchedVertex = matchedVertex;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      if (in.readBoolean()) {
        matchedVertex = new LongWritable();
        matchedVertex.readFields(in);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(matchedVertex != null);
      if (matchedVertex != null) {
        matchedVertex.write(out);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(matchedVertex != null ? matchedVertex.get() : "null");
      return sb.toString();
    }

  }

  /**
   * Message for bipartite matching.
   */
  public static class Message implements Writable {

    /**
     * Id of the vertex sending this message.
     */
    private LongWritable senderVertex;
    /**
     * Whether this message is a match request (null), or a message that grants
     * (true) or denies (false) another one.
     */
    private BooleanWritable isGranting;

    /**
     * Default constructor.
     */
    public Message() {
    }

    /**
     * Constructs a match request message.
     *
     * @param vertex
     *          Sending vertex
     */
    public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
      senderVertex = vertex.getId();
    }

    /**
     * Constructs a match granting or denying message.
     *
     * @param vertex
     *          Sending vertex
     * @param isGranting
     *          True iff it is a granting message
     */
    public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex,
      BooleanWritable isGranting) {
      this(vertex);
      this.isGranting = isGranting;
    }

    /**
     * Constructs a match granting or denying message.
     *
     * @param vertex
     *          Sending vertex
     * @param isGranting
     *          True iff it is a granting message
     */
    public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex,
      boolean isGranting) {
      this(vertex, new BooleanWritable(isGranting));
    }

    public LongWritable getSenderVertex() {
      return senderVertex;
    }

    public void setSenderVertex(LongWritable senderVertex) {
      this.senderVertex = senderVertex;
    }

    public BooleanWritable isGranting() {
      return isGranting;
    }

    @Override
    public String toString() {
      if (isGranting == null) {
        return "MATCH_REQUEST from " + senderVertex;
      } else if (isGranting.get()) {
        return "MATCH_GRANTED from " + senderVertex;
      } else {
        return "MATCH_DENIED from " + senderVertex;
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      if (in.readBoolean()) {
        senderVertex = new LongWritable();
        senderVertex.readFields(in);
      } else {
        senderVertex = null;
      }
      if (in.readBoolean()) {
        isGranting = new BooleanWritable();
        isGranting.readFields(in);
      } else {
        isGranting = null;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(senderVertex != null);
      if (senderVertex != null) {
        senderVertex.write(out);
      }
      out.writeBoolean(isGranting != null);
      if (isGranting != null) {
        isGranting.write(out);
      }
    }

  }

}
