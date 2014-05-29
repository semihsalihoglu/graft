package org.apache.giraph.debugger.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.giraph.graph.Computation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Contains common utility classes shared one or more of:
 * <ul>
 *   <li> Graft instrumenter and the 
 *   <li> server that serves data to Graft GUI by talking to HDFS
 *   <li> Wrapper classes around the scenario protocol buffers that are stored under
 *        {@link org.apache.giraph.debugger.utils}.
 * </ul>
 * @author semihsalihoglu
 */
public class DebugUtils {

  public static final String TRACE_ROOT = System.getProperty("giraph.debugger.traceRootAtHDFS", "/giraph-debug-traces");
  public static final String JARCACHE_HDFS = System.getProperty("giraph.debugger.jobCacheAtHDFS", TRACE_ROOT + "/jars");
  public static final String JARCACHE_LOCAL = System.getProperty("giraph.debugger.jobCacheLocal", System.getenv("HOME") + "/.giraph-debug/jars");

  // Enumeration of different trace files Graft saves in HDFS.
  public enum DebugTrace {
    VERTEX_REGULAR, VERTEX_EXCEPTION, VERTEX_ALL, 
    INTEGRITY_MESSAGE_ALL, INTEGRITY_MESSAGE_SINGLE_VERTEX, INTEGRITY_VERTEX, 
    MASTER_REGULAR, MASTER_EXCEPTION, MASTER_ALL,
    JAR_SIGNATURE
  }

  /**
   * Makes a clone of a writable object. Giraph sometimes reuses and overwrites the bytes
   * inside {@link Writable} objects. For example, when reading the incoming messages inside
   * a {@link Computation} class through the iterator Giraph supplies, Giraph uses only one object.
   * Therefore in order to keep a pointer to particular object, we need to clone it.
   * 
   * @param writableToClone Writable object to clone.
   * @param clazz Class of writableToClone.
   * @return a clone of writableToClone.
   */
  public static <T extends Writable> T makeCloneOf(T writableToClone, Class<T> clazz) {
    T idCopy = newInstance(clazz);
    // Return value is null if clazz is assignable to NullWritable.
    if (idCopy == null) {
      return writableToClone;
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      writableToClone.write(dataOutputStream);
    } catch (IOException e) {
      // Throwing a runtime exception because the methods that call other methods
      // such as addNeighborWrapper or addOutgoingMessageWrapper, implement abstract classes
      // or interfaces of Giraph that we can't edit to include a throws statement.
      throw new RuntimeException(e);
    }
    // 
    if (byteArrayOutputStream.toByteArray() != null) {
      WritableUtils.readFieldsFromByteArray(byteArrayOutputStream.toByteArray(), idCopy);
      byteArrayOutputStream.reset();
    }
    return idCopy;
  }

  /**
   * Instantiates a new object from the given class.
   */
  public static <T> T newInstance(Class<T> theClass) {
    return NullWritable.class.isAssignableFrom(theClass) 
        ? null : ReflectionUtils.newInstance(theClass);
  }

  
  /**
   * Returns the full trace file name for the given type of debug trace. One or more of the passed
   * arguments will be used in the file name.
   */
  public static String getFullTraceFileName(DebugTrace debugTrace, String jobId, Long superstepNo,
    String vertexId, Integer taskId) {
    return getTraceFileRoot(jobId) + "/"
      + getTraceFileName(debugTrace, superstepNo, vertexId, taskId);
  }
  
  /**
   * A convenience method around
   * {@link #getFullTraceFileName(DebugTrace, String, Long, String, Integer)}.
   */
  public static String getMessageIntegrityAllTraceFullFileName(long superstepNo, String jobId,
    int taskId) {
    return getFullTraceFileName(DebugTrace.INTEGRITY_MESSAGE_ALL, jobId, superstepNo,
      null /* no vertex Id */, taskId);
  }
  
  /**
   * A convenience method around
   * {@link #getFullTraceFileName(DebugTrace, String, Long, String, Integer)}.
   */
  public static String getFullMasterTraceFileName(DebugTrace masterDebugTrace, String jobId, 
    Long superstepNo) {
    return getFullTraceFileName(masterDebugTrace, jobId, superstepNo,
      null /* no vertex Id */, null /* no trace Id */);
  }

  /**
   * A convenience method around
   * {@link #getFullTraceFileName(DebugTrace, String, Long, String, Integer)}.
   */
  public static String getFullTraceFileName(DebugTrace debugTrace, String jobId, Long superstepNo,
    String vertexId) {
    return getFullTraceFileName(debugTrace, jobId, superstepNo, vertexId,
      null /* no trace Id */);
  }

  private static String getTraceFileName(DebugTrace debugTrace, Long superstepNo, String vertexId,
    Integer taskId) {
    String format = getTraceFileFormat(debugTrace);
    switch (debugTrace) {
    case VERTEX_REGULAR:
      return String.format(format, superstepNo, vertexId);
    case VERTEX_EXCEPTION:
      return String.format(format, superstepNo, vertexId);
    case INTEGRITY_MESSAGE_ALL:
      return String.format(format, taskId, superstepNo);
    case INTEGRITY_MESSAGE_SINGLE_VERTEX:
      return String.format(format, superstepNo, vertexId);
    case INTEGRITY_VERTEX:
      return String.format(format, superstepNo, vertexId);
    case MASTER_REGULAR:
      return String.format(format, superstepNo);
    case MASTER_EXCEPTION:
      return String.format(format, superstepNo);
    default:
      return null;
    }
  }
  
  /**
   * Returns the file name of the trace file given the three parameters. Pass
   * arbitrary vertexId for traces which do not require a vertexId.
   */
  public static String getTraceFileFormat(DebugTrace debugTrace) {
    switch (debugTrace) {
    case VERTEX_REGULAR:
      return "reg_stp_%d_vid_%s.tr";
    case VERTEX_EXCEPTION:
      return "err_stp_%d_vid_%s.tr";
    case VERTEX_ALL:
      return "(reg|err)_stp_%d_vid_%s.tr"; 
    case INTEGRITY_MESSAGE_ALL:
      return "task_%s_msg_intgrty_stp_%d.tr";
    case INTEGRITY_MESSAGE_SINGLE_VERTEX:
      return "msg_intgrty_stp_%d_vid_%s.tr";
    case INTEGRITY_VERTEX:
      return "vv_intgrty_stp_%d_vid_%s.tr";
     case MASTER_REGULAR:
      return "master_reg_stp_%d.tr";
    case MASTER_EXCEPTION:
      return "master_err_stp_%d.tr";
    case MASTER_ALL:
      return "master_(reg|err)_stp_%d.tr";
    default:
      throw new IllegalArgumentException("DebugTrace not supported.");
    }
  }

  /**
   * Returns the root directory of the trace files for the given job.
   */
  public static String getTraceFileRoot(String jobId) {
    return String.format("%s/%s", DebugUtils.TRACE_ROOT, jobId);
  }
}