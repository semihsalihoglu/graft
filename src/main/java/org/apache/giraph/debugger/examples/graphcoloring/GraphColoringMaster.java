package org.apache.giraph.debugger.examples.graphcoloring;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class GraphColoringMaster extends DefaultMasterCompute {

  public static final String PHASE = "phase";
  public static final String COLOR_TO_ASSIGN = "colorToAssign";
  public static final String NUM_VERTICES_COLORED = "numVerticesColored";
  public static final String NUM_VERTICES_UNKNOWN = "numVerticesUnknown";

  public static enum Phase {
    LOTTERY, CONFLICT_RESOLUTION, EDGE_CLEANING, COLOR_ASSIGNMENT,
  }

  private int colorToAssign;
  private Phase nextPhase;

  @Override
  public void initialize() throws InstantiationException,
    IllegalAccessException {
    registerPersistentAggregator(COLOR_TO_ASSIGN, IntMaxAggregator.class);
    colorToAssign = VertexValue.NO_COLOR;
    registerPersistentAggregator(PHASE, IntMaxAggregator.class);
    nextPhase = Phase.LOTTERY;

    registerPersistentAggregator(NUM_VERTICES_COLORED, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_UNKNOWN, LongSumAggregator.class);
    setAggregatedValue(NUM_VERTICES_UNKNOWN, new LongWritable(
      getTotalNumVertices()));
  }

  @Override
  public void compute() {
    // We can assign colors to the vertices in the independent set if there
    // are no remaining UNKNOWNs at a LOTTERY phase.
    if (nextPhase == Phase.LOTTERY) {
      long numUnknown = ((LongWritable) getAggregatedValue(NUM_VERTICES_UNKNOWN))
        .get();
      if (numUnknown == 0) {
        nextPhase = Phase.COLOR_ASSIGNMENT;
      }
    }

    // Set an aggregator to communicate what nextPhase we're in to all vertices.
    setAggregatedValue(PHASE, new IntWritable(nextPhase.ordinal()));

    switch (nextPhase) {
    case LOTTERY:
      long numColored = ((LongWritable) getAggregatedValue(NUM_VERTICES_COLORED))
        .get();
      if (numColored == getTotalNumVertices()) {
        // Halt when all vertices are colored.
        haltComputation();
        return;
      } else {
        // Otherwise, move to conflict resolution after selecting a set of
        // vertices.
        nextPhase = Phase.CONFLICT_RESOLUTION;
      }
      break;

    case CONFLICT_RESOLUTION:
      // After resolving conflicts, move on to edge cleaning.
      nextPhase = Phase.EDGE_CLEANING;
      break;

    case EDGE_CLEANING:
      // Repeat finding independent sets after cleaning edges.
      // remaining.
      nextPhase = Phase.LOTTERY;
      break;

    case COLOR_ASSIGNMENT:
      // Set an aggregator telling each IN_SET vertex what color to assign.
      setAggregatedValue(COLOR_TO_ASSIGN, new IntWritable(colorToAssign++));
      // Start a new cycle of finding maximal independent sets, after assigning
      // colors.
      nextPhase = Phase.LOTTERY;
      break;

    default:
      throw new IllegalStateException();
    }
  }
}
