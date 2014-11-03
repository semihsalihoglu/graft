package org.apache.giraph.debugger.examples.graphcoloring;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class GraphColoringMaster extends DefaultMasterCompute {

  public static final String PHASE = "phase";
  public static final String COLOR_TO_ASSIGN = "colorToAssign";
  public static final String NUM_VERTICES_COLORED = "numVerticesColored";
  public static final String NUM_VERTICES_UNKNOWN = "numVerticesUnknown";
  public static final String NUM_VERTICES_IN_SET = "numVerticesInSet";
  public static final String NUM_VERTICES_NOT_IN_SET = "numVerticesNotInSet";
  public static final String NUM_VERTICES_TENTATIVELY_IN_SET = "numVerticesTentativelyInSet";

  public static enum Phase {
    LOTTERY, CONFLICT_RESOLUTION, EDGE_CLEANING, COLOR_ASSIGNMENT,
  }

  private int colorToAssign;
  private Phase phase;

  @Override
  public void initialize() throws InstantiationException,
    IllegalAccessException {
    registerPersistentAggregator(COLOR_TO_ASSIGN, IntMaxAggregator.class);
    colorToAssign = VertexValue.NO_COLOR;
    registerPersistentAggregator(PHASE, IntMaxAggregator.class);
    phase = null;

    registerPersistentAggregator(NUM_VERTICES_COLORED, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_UNKNOWN, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_TENTATIVELY_IN_SET, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_NOT_IN_SET, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_IN_SET, LongSumAggregator.class);
  }

  @Override
  public void compute() {
    if (phase != null) {
      switch (phase) {
      case LOTTERY:
        // Move to conflict resolution after selecting a set of vertices.
        phase = Phase.CONFLICT_RESOLUTION;
        break;

      case CONFLICT_RESOLUTION:
        // After resolving conflicts, move on to edge cleaning.
        phase = Phase.EDGE_CLEANING;
        break;

      case EDGE_CLEANING:
        // We can assign colors to the vertices in the independent set if there
        // are no remaining UNKNOWNs at a LOTTERY phase.
        long numUnknown = ((LongWritable) getAggregatedValue(NUM_VERTICES_UNKNOWN))
          .get();
        if (numUnknown == 0) {
          // Set an aggregator telling each IN_SET vertex what color to assign.
          setAggregatedValue(COLOR_TO_ASSIGN, new IntWritable(++colorToAssign));
          phase = Phase.COLOR_ASSIGNMENT;
        } else {
          // Repeat finding independent sets after cleaning edges.
          // remaining.
          phase = Phase.LOTTERY;
        }
        break;

      case COLOR_ASSIGNMENT:
        System.out.println(getSuperstep() + ": MASTER phase: " + phase +
          ", color " + colorToAssign);
        long numColored = ((LongWritable) getAggregatedValue(NUM_VERTICES_COLORED))
          .get();
        if (numColored == getTotalNumVertices()) {
          // Halt when all vertices are colored.
          haltComputation();
          return;
        }
        // Start a new cycle of finding maximal independent sets, after
        // assigning colors.
        phase = Phase.LOTTERY;
        break;

      default:
        throw new IllegalStateException();
      }
    } else {
      // First superstep, enter into lottery.
      phase = Phase.LOTTERY;
    }

    // Set an aggregator to communicate what phase we're in to all vertices.
    setAggregatedValue(PHASE, new IntWritable(phase.ordinal()));
  }
}
