package org.apache.giraph.debugger.examples.randomwalk;

import java.io.IOException;
import java.util.Random;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class RandomWalkComputation extends
  BasicComputation<LongWritable, IntWritable, NullWritable, IntWritable> {

  private static final int DEFAULT_NUM_WALKERS = 100;
  private static final int DEFAULT_LENGTH_OF_WALK = 20;

  private short[] messagesToNeighbors = new short[2];
  private int initialNumWalkers;
  private int lengthOfWalk;

  @Override
  public void initialize(
    GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, IntWritable, NullWritable> workerClientRequestProcessor,
    GraphTaskManager<LongWritable, IntWritable, NullWritable> graphTaskManager,
    WorkerAggregatorUsage workerAggregatorUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor,
      graphTaskManager, workerAggregatorUsage, workerContext);

    initialNumWalkers = getConf().getInt(
      getClass().getName() + ".initialNumWalkers", DEFAULT_NUM_WALKERS);
    lengthOfWalk = getConf().getInt(getClass().getName() + ".walkLength",
      DEFAULT_LENGTH_OF_WALK);
  }

  @Override
  public void compute(Vertex<LongWritable, IntWritable, NullWritable> vertex,
    Iterable<IntWritable> messages) throws IOException {
    // Halt after the walk reaches a certain length.
    if (getSuperstep() > lengthOfWalk) {
      vertex.voteToHalt();
      return;
    }
    short numWalkersHere = 0;
    if (getSuperstep() == 0) {
      // At the first superstep, start from an initial number of walkers.
      numWalkersHere += initialNumWalkers;
    } else {
      // Otherwise, count the number of walkers arrived at this vertex.
      for (IntWritable messageValue : messages) {
        numWalkersHere += messageValue.get();
      }
    }
    vertex.setValue(new IntWritable(numWalkersHere));
    moveWalkersToNeighbors(numWalkersHere, vertex);
  }

  private void moveWalkersToNeighbors(int numMessagesToSend,
    Vertex<LongWritable, IntWritable, NullWritable> vertex) {
    Iterable<Edge<LongWritable, NullWritable>> edges = vertex.getEdges();
    int neighborsLength = vertex.getNumEdges();
    if (messagesToNeighbors.length < neighborsLength) {
      messagesToNeighbors = new short[neighborsLength];
    } else {
      for (int i = 0; i < neighborsLength; ++i) {
        messagesToNeighbors[i] = 0;
      }
    }
    Random random = new Random();
    if (neighborsLength == 0) {
      // When there's no out-edge, let each walker jump to a random vertex in
      // the graph.
      for (int i = 0; i < numMessagesToSend; ++i) {
        sendMessage(
          new LongWritable(random.nextInt((int) getTotalNumVertices())),
          new IntWritable(1));
      }
    } else {
      // Otherwise, distribute the walkers on this vertex to each neighbor.
      for (int i = 0; i < numMessagesToSend; ++i) {
        int neighborIdIndex = random.nextInt(neighborsLength);
        messagesToNeighbors[neighborIdIndex] += 1;
      }
      // Then, send out messages containing the number of walkers.
      int i = 0;
      for (Edge<LongWritable, NullWritable> edge : edges) {
        if (messagesToNeighbors[i] != 0) {
          sendMessage(edge.getTargetVertexId(), new IntWritable(
            messagesToNeighbors[i]));
        }
        i++;
      }
    }
  }

}
