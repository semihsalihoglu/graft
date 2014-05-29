package org.apache.giraph.debugger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/** 
 * This class is used by programmers to configure what they want to be debugged. Programmers
 * can either extend this class and implement their own debug configurations or use a few hadoop
 * config parameters to use this one. If programmers implement their own config, they can do
 * the following:
 * <ul>
 *  <li> Configure which vertices to debug by looking at the whole {@link Vertex} object.
 *  <li> Configure which supersteps to debug.
 *  <li> Add a message integrity constraint by setting {@link #shouldCheckMessageIntegrity()} to
 *       true and then overriding
 *       {@link #isMessageCorrect(WritableComparable, WritableComparable, Writable)}.
 *  <li> Add a vertex value integrity constraint by setting
 *       {@link #shouldCheckVertexValueIntegrity()} and then overriding
 *       {@link #isVertexValueCorrect(WritableComparable, Writable)}.
 * </ul>
 * 
 * If instead the programmers use this class without extending it, they can configure it as
 * follows:
 * <ul>
 *   <li> By passing -D{@link #VERTICES_TO_DEBUG_FLAG}=v1,v2,..,vn, specify a set of
 *        integer or long vertex IDs to debug. The {@link Computation} class has to have either a
 *        {@link LongWritable} or {@link IntWritable}. By default no vertices are debugged.
 *   <li> By passing -D{@link #DEBUG_NEIGHBORS_FLAG}=true/false specify whether the in-neighbors
 *        of vertices that were configured to be debugged should also be debugged. By default this
 *        flag is set to false.
 *   <li> By passing -D{@link #SUPERSTEPS_TO_DEBUG_FLAG}=s1,s2,...,sm specify a set of supersteps
 *        to debug. By default all supersteps are debugged.
 * </ul>
 * 
 * Note that if programmers use this class directly, then by default the debugger will capture
 * exceptions.
 *
 * @author semihsalihoglu
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
@SuppressWarnings({ "rawtypes" })
public class DebugConfig<I extends WritableComparable, V extends Writable,
  E extends Writable, M1 extends Writable, M2 extends Writable> {

  private static final String SUPERSTEP_DELIMITER = ":";
  private static final String VERTEX_ID_DELIMITER = ":";
  
  public static final String VERTICES_TO_DEBUG_FLAG = "giraph.debugger.verticesToDebug";
  public static final String DEBUG_NEIGHBORS_FLAG = "giraph.debugger.debugNeighbors";
  public static final String SUPERSTEPS_TO_DEBUG_FLAG = "giraph.debugger.superstepsToDebug";
  public static final String DEBUG_ALL_VERTICES_FLAG = "giraph.debugger.debugAllVertices";

  protected static final Logger LOG = Logger.getLogger(DebugConfig.class);

  private Set<I> verticesToDebugSet;
  private Set<Long> superstepsToDebugSet;
  private boolean debugNeighborsOfVerticesToDebug;
  private boolean debugAllVertices = false;

  public DebugConfig() {
    verticesToDebugSet = null;
    debugAllVertices = false;
    debugNeighborsOfVerticesToDebug = false;
    superstepsToDebugSet = null;
  }
  
  public final void readConfig(GiraphConfiguration config) {
    this.debugNeighborsOfVerticesToDebug = config.getBoolean(DEBUG_NEIGHBORS_FLAG,
      false);

    String superstepsToDebugStr = config.get(SUPERSTEPS_TO_DEBUG_FLAG, null);
    if (superstepsToDebugStr == null) {
      superstepsToDebugSet = null;
    } else {
      String[] superstepsToDebugArray = superstepsToDebugStr.split(SUPERSTEP_DELIMITER);
      superstepsToDebugSet = new HashSet<>();
      for (String superstepStr : superstepsToDebugArray) {
        superstepsToDebugSet.add(Long.valueOf(superstepStr));
      }
    }

    debugAllVertices = config.getBoolean(DEBUG_ALL_VERTICES_FLAG, false);
    if (!debugAllVertices) {
      String verticesToDebugStr = config.get(VERTICES_TO_DEBUG_FLAG, null);
      Class<? extends Computation> userComputationClass = config.getComputationClass();
      Class<?>[] typeArguments = ReflectionUtils.getTypeArguments(Computation.class,
        userComputationClass);
      Class<?> idType = typeArguments[0];
      if (verticesToDebugStr != null) {
        String[] verticesToDebugArray = verticesToDebugStr.split(VERTEX_ID_DELIMITER);
        this.verticesToDebugSet = new HashSet<>();
        for (String idString : verticesToDebugArray) {
          if (LongWritable.class.isAssignableFrom(idType)) {
            verticesToDebugSet.add((I) new LongWritable(Long.valueOf(idString)));
          } else if (IntWritable.class.isAssignableFrom(idType)) {
            verticesToDebugSet.add((I) new IntWritable(Integer.valueOf(idString)));
          } else {
            throw new IllegalArgumentException("When using the giraph.debugger.verticesToDebug"
              + " argument, the vertex IDs of the computation class needs to be LongWritable"
              + " or IntWritable.");
          }
        }
      }
    }
    LOG.info("Printing DebugConfig:");
    LOG.info(this.toString());
    LOG.info("End of Printing DebugConfig.");
  }

  public boolean shouldDebugSuperstep(long superstepNo) {
    return superstepsToDebugSet == null || superstepsToDebugSet.contains(superstepNo);
  }
  
  public boolean shouldDebugVertex(Vertex<I, V, E> vertex) {
    if (debugAllVertices) {
      return true;
    } 
    // Should not debug all vertices. Check if any vertices were special cased.
    if (verticesToDebugSet == null) {
      return false;     
    } else {
      return verticesToDebugSet.contains(vertex.getId()) || (debugNeighborsOfVerticesToDebug &&
        isVertexANeighborOfAVertexToDebug(vertex));
    }
  }
  
  private boolean isVertexANeighborOfAVertexToDebug(Vertex<I, V, E> vertex) {
    for (Edge<I, E> edge : vertex.getEdges()) {
      if (verticesToDebugSet.contains(edge.getTargetVertexId())) {
        return true;
      }
    }
    return false;
  }

  public boolean shouldCatchExceptions() {
    return true;
  }
  
  public boolean shouldCheckMessageIntegrity() {
    return false;
  }
  
  public boolean isMessageCorrect(I srcId, I dstId, M1 message) {
    return true;
  }

  public boolean shouldCheckVertexValueIntegrity() {
    return false;
  }
  
  public boolean isVertexValueCorrect(I vertexId, V value) {
    return true;
  }
  
  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("superstepsToDebug: " + (superstepsToDebugSet == null ? "all supersteps" :
      Arrays.toString(superstepsToDebugSet.toArray())));
    stringBuilder.append("verticesToDebug: " + (verticesToDebugSet == null ? null :
      Arrays.toString(verticesToDebugSet.toArray())));
    stringBuilder.append("debugNeighborsOfVerticesToDebug: " + debugNeighborsOfVerticesToDebug);
    stringBuilder.append("shouldCatchExceptions: " + shouldCatchExceptions());
    stringBuilder.append("shouldCheckMessageIntegrity: " + shouldCheckMessageIntegrity());
    stringBuilder.append("shouldCheckVertexValueIntegrity: "
      + shouldCheckVertexValueIntegrity());
    return stringBuilder.toString();
  }
}