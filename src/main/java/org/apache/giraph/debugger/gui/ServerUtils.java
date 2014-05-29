package org.apache.giraph.debugger.gui;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.giraph.debugger.utils.AggregatedValueWrapper;
import org.apache.giraph.debugger.utils.DebugUtils;
import org.apache.giraph.debugger.utils.DebugUtils.DebugTrace;
import org.apache.giraph.debugger.utils.ExceptionWrapper;
import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper.NeighborWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper.OutgoingMessageWrapper;
import org.apache.giraph.debugger.utils.MsgIntegrityViolationWrapper;
import org.apache.giraph.debugger.utils.MsgIntegrityViolationWrapper.ExtendedOutgoingMessageWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import sun.security.ssl.Debug;

/*
 * Utility methods for Debugger Server.
 */
public class ServerUtils {
  public static final String JOB_ID_KEY = "jobId";
  public static final String VERTEX_ID_KEY = "vertexId";
  public static final String SUPERSTEP_ID_KEY = "superstepId";
  public static final String INTEGRITY_VIOLATION_TYPE_KEY = "type";
  public static final String TASK_ID_KEY = "taskId";
  public static final String ADJLIST_KEY = "adjList";

  /*
   * Returns parameters of the URL in a hash map. For instance,
   * http://localhost:9000/?key1=val1&key2=val2&key3=val3
   */
  public static HashMap<String, String> getUrlParams(String rawUrl)
    throws UnsupportedEncodingException {
    HashMap<String, String> paramMap = new HashMap<String, String>();

    if (rawUrl != null) {
      String[] params = rawUrl.split("&");
      for (String param : params) {
        String[] parts = param.split("=");
        String paramKey = URLDecoder.decode(parts[0], "UTF-8");
        String paramValue = URLDecoder.decode(parts[1], "UTF-8");
        paramMap.put(paramKey, paramValue);
      }
    }
    return paramMap;
  }

  /*
   * Returns the HDFS FileSystem reference. 
   * Note: We assume that the classpath contains the Hadoop's conf directory or the core-site.xml
   * and hdfs-site.xml configuration directories.
   */
  public static FileSystem getFileSystem() throws IOException {
    Configuration configuration = new Configuration();
    return FileSystem.get(configuration);
  }

  public static URL getCachedJobJarPath(String jobId) throws IOException {
    // read the jar signature file under the TRACE_ROOT/jobId/
    Path jarSignaturePath = new Path(DebugUtils.getTraceFileRoot(jobId) + "/"
      + "jar.signature");
    FileSystem fs = getFileSystem();
    String jarSignature = IOUtils.readLines(fs.open(jarSignaturePath)).get(0);
    // check if jar is already in JARCACHE_LOCAL
    File localFile = new File(DebugUtils.JARCACHE_LOCAL + "/" + jarSignature + ".jar");
    if (!localFile.exists()) {
      // otherwise, download from HDFS
      Path hdfsPath = new Path(fs.getUri().resolve(DebugUtils.JARCACHE_HDFS + "/" + jarSignature + ".jar"));
      Logger.getLogger(ServerUtils.class).info("Copying from HDFS: " + hdfsPath + " to " + localFile);
      localFile.getParentFile().mkdirs();
      fs.copyToLocalFile(hdfsPath, new Path(localFile.toURI()));
    }
    return localFile.toURI().toURL();
  }
  
  /*
   * Returns the path of the vertex trace file on HDFS.
   * @param debugTrace - Must be one of VERTEX_* or
   * INTEGRITY_VERTEX types. 
   */
  public static String getVertexTraceFilePath(String jobId, long superstepNo, 
    String vertexId, DebugTrace debugTrace) {
    assert EnumSet.of(DebugTrace.VERTEX_EXCEPTION, 
      DebugTrace.VERTEX_REGULAR, DebugTrace.INTEGRITY_VERTEX).contains(debugTrace);
    return String.format("%s/%s",
        DebugUtils.getTraceFileRoot(jobId),
        String.format(DebugUtils.getTraceFileFormat(debugTrace), superstepNo, vertexId));
  }
  
  /*
   * Returns the path of the vertex trace file on HDFS.
   * @param debugTrace - Must be INTEGRITY_MESSAGE. 
   */
  public static String getIntegrityTraceFilePath(String jobId, String taskId, 
    long superstepNo, DebugTrace debugTrace) {
    assert EnumSet.of(DebugTrace.INTEGRITY_MESSAGE_ALL).contains(debugTrace);
    return String.format("%s/%s",
        DebugUtils.getTraceFileRoot(jobId),
        String.format(DebugUtils.getTraceFileFormat(debugTrace), taskId, superstepNo));
  }
  
  /*
   * Returns the path of the master compute trace file on HDFS.
   */
  public static String getMasterTraceFilePath(String jobId, long superstepNo, 
    DebugTrace debugTrace) {
    assert EnumSet.of(DebugTrace.MASTER_ALL, DebugTrace.MASTER_EXCEPTION, 
      DebugTrace.MASTER_REGULAR).contains(debugTrace);
    return String.format("%s/%s",
        DebugUtils.getTraceFileRoot(jobId),
        String.format(DebugUtils.getTraceFileFormat(debugTrace), superstepNo));
  }

  /*
   * Reads the protocol buffer trace corresponding to the given jobId,
   * superstepNo and vertexId and returns the giraphScenarioWrapper.
   * @param jobId : ID of the job debugged.
   * @param superstepNo: Superstep number debugged.
   * @param vertexId - ID of the vertex debugged. Returns GiraphScenarioWrapper.
   * @param [debugTrace] - Can be either REGULAR, EXCEPTION OR ALL_VERTICES. In case
   * of null, returns whichever trace is available.
   */
  public static GiraphVertexScenarioWrapper readScenarioFromTrace(String jobId, long superstepNo,
    String vertexId, DebugTrace debugTrace) throws IOException, ClassNotFoundException, 
    InstantiationException, IllegalAccessException {
    if (!EnumSet.of(DebugTrace.VERTEX_ALL, DebugTrace.VERTEX_EXCEPTION, 
      DebugTrace.VERTEX_REGULAR).contains(debugTrace)) {
      // Throw exception for unsupported debug trace. 
      throw new IllegalArgumentException(
        "DebugTrace type is invalid. Use REGULAR, EXCEPTION or ALL_VERTICES");
    }
    FileSystem fs = ServerUtils.getFileSystem();
    GiraphVertexScenarioWrapper giraphScenarioWrapper = new GiraphVertexScenarioWrapper();
    // If debugTrace is regular or null, try reading the regular trace first.
    if (debugTrace == DebugTrace.VERTEX_REGULAR || debugTrace == DebugTrace.VERTEX_ALL) {
      String traceFilePath = ServerUtils.getVertexTraceFilePath(jobId, superstepNo, 
        vertexId, DebugTrace.VERTEX_REGULAR);
      try {
        giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath, getCachedJobJarPath(jobId));
        // If scenario is found, return it. 
        return giraphScenarioWrapper;
      } catch(FileNotFoundException e) {
        // If debugTrace was null, ignore this exception since 
        // we will try reading exception trace later.
        if ( debugTrace == DebugTrace.VERTEX_ALL) {
          Debug.println("readScenarioFromTrace", "Regular file not found. Ignoring.");
        } else {
          throw e;
        }
      }
    } 
    // This code is reached only when debugTrace = exception or null. 
    // In case of null, it is only reached when regular trace is not found already.
    String traceFilePath = ServerUtils.getVertexTraceFilePath(jobId, superstepNo, 
      vertexId, DebugTrace.VERTEX_EXCEPTION);
    giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath, getCachedJobJarPath(jobId));
    return giraphScenarioWrapper;
  }
  
  /*
   * Reads the master protocol buffer trace corresponding to the given jobId
   * and superstepNo and returns the GiraphMasterScenarioWrapper object.
   * @param jobId : ID of the job debugged.
   * @param superstepNo: Superstep number debugged.
   * @param [debugTrace] - Can be either MASTER_REGULAR, MASTER_EXCEPTION OR MASTER_ALL. In case
   * of MASTER_ALL, returns whichever trace is available.
   */
  public static GiraphMasterScenarioWrapper readMasterScenarioFromTrace(String jobId,
    long superstepNo, DebugTrace debugTrace) throws IOException, 
    ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (!EnumSet.of(DebugTrace.MASTER_ALL, DebugTrace.MASTER_EXCEPTION, 
      DebugTrace.MASTER_REGULAR).contains(debugTrace)) {
      // Throw exception for unsupported debug trace. 
      throw new IllegalArgumentException(
        "DebugTrace type is invalid. Use REGULAR, EXCEPTION or ALL_VERTICES");
    }
    FileSystem fs = ServerUtils.getFileSystem();
    GiraphMasterScenarioWrapper giraphScenarioWrapper = new GiraphMasterScenarioWrapper();
    // For each superstep, there is either a "regular" master trace (saved in
    // master_reg_stp_i.tr files), or an "exception" master trace (saved in
    // master_err_stp_i.tr files). We first check to see if a regular master
    // trace is available. If not, then we check to see if an exception master
    // trace is available.
    if (debugTrace == DebugTrace.MASTER_REGULAR || debugTrace == DebugTrace.MASTER_ALL) {
      String traceFilePath = ServerUtils.getMasterTraceFilePath(jobId, superstepNo,
        DebugTrace.MASTER_REGULAR);
      try {
        giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath, getCachedJobJarPath(jobId));
        // If scenario is found, return it. 
        return giraphScenarioWrapper;
      } catch(FileNotFoundException e) {
        // If debugTrace was null, ignore this exception since 
        // we will try reading exception trace later.
        if ( debugTrace == DebugTrace.MASTER_ALL) {
          Debug.println("readMasterScenarioFromTrace", "Regular file not found. Ignoring.");
        } else {
          throw e;
        }
      }
    } 
    // This code is reached only when debugTrace = exception or null. 
    // In case of null, it is only reached when regular trace is not found already.
    String traceFilePath = ServerUtils.getMasterTraceFilePath(jobId, superstepNo,
      DebugTrace.MASTER_EXCEPTION);
    giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath, getCachedJobJarPath(jobId));
    return giraphScenarioWrapper;
  }
  
  /*
   * Returns the MessageIntegrityViolationWrapper from trace file.
   */
  public static MsgIntegrityViolationWrapper readMsgIntegrityViolationFromTrace(String jobId,
    String taskId, long superstepNo) throws IOException, ClassNotFoundException, 
    InstantiationException, IllegalAccessException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getIntegrityTraceFilePath(jobId, taskId, 
      superstepNo, DebugTrace.INTEGRITY_MESSAGE_ALL);
    MsgIntegrityViolationWrapper msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper();
    msgIntegrityViolationWrapper.loadFromHDFS(fs, traceFilePath, getCachedJobJarPath(jobId));
    return msgIntegrityViolationWrapper;
  }

  /*
   * Returns the MessageIntegrityViolationWrapper from trace file.
   */
  public static GiraphVertexScenarioWrapper readVertexIntegrityViolationFromTrace(
    String jobId, long superstepNo, String vertexId) throws IOException, 
    ClassNotFoundException, InstantiationException, IllegalAccessException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getVertexTraceFilePath(jobId, superstepNo, 
      vertexId, DebugTrace.INTEGRITY_VERTEX);
    GiraphVertexScenarioWrapper giraphScenarioWrapper = new GiraphVertexScenarioWrapper();
    giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath);
    return giraphScenarioWrapper;
  }

  /*
   * Converts a Giraph Scenario (giraphScenarioWrapper object) to JSON
   * (JSONObject)
   * @param giraphScenarioWrapper : Giraph Scenario object.
   */
  public static JSONObject scenarioToJSON(GiraphVertexScenarioWrapper giraphScenarioWrapper)
    throws JSONException {
    VertexContextWrapper contextWrapper = giraphScenarioWrapper.getContextWrapper();
    JSONObject scenarioObj = new JSONObject();
    scenarioObj.put("vertexId", contextWrapper.getVertexIdWrapper());
    scenarioObj.put("vertexValue", contextWrapper.getVertexValueAfterWrapper());
    JSONObject outgoingMessagesObj = new JSONObject();
    JSONArray neighborsList = new JSONArray();
    // Add outgoing messages.
    for (Object outgoingMessage : contextWrapper.getOutgoingMessageWrappers()) {
      OutgoingMessageWrapper outgoingMessageWrapper = (OutgoingMessageWrapper) outgoingMessage;
      outgoingMessagesObj.put(outgoingMessageWrapper.destinationId.toString(),
        outgoingMessageWrapper.message.toString());
    }
    // Add incoming messages.
    ArrayList<String> incomingMessagesList = new ArrayList<String>();
    for (Object incomingMessage : contextWrapper.getIncomingMessageWrappers()) {
      incomingMessagesList.add(incomingMessage.toString());
    }
    // Add neighbors.
    for (Object neighbor : contextWrapper.getNeighborWrappers()) {
      JSONObject neighborObject = new JSONObject();
      NeighborWrapper neighborWrapper = (NeighborWrapper) neighbor;
      neighborObject.put("neighborId", neighborWrapper.getNbrId());
      neighborObject.put("edgeValue", neighborWrapper.getEdgeValue());
      neighborsList.put(neighborObject);
    }
    scenarioObj.put("outgoingMessages", outgoingMessagesObj);
    scenarioObj.put("incomingMessages", incomingMessagesList);
    scenarioObj.put("neighbors", neighborsList);
    // Add exception, if present.
    if (giraphScenarioWrapper.hasExceptionWrapper()) {
      JSONObject exceptionObj = new JSONObject();
      ExceptionWrapper exceptionWrapper = giraphScenarioWrapper.getExceptionWrapper();
      exceptionObj.put("message", exceptionWrapper.getErrorMessage());
      exceptionObj.put("stackTrace", exceptionWrapper.getStackTrace());
      scenarioObj.put("exception", exceptionObj);
    }
    JSONObject aggregateObj = new JSONObject();
    for (Object aggregatedValue : contextWrapper.getCommonVertexMasterContextWrapper()
      .getPreviousAggregatedValues()) {
      AggregatedValueWrapper aggregatedValueWrapper = (AggregatedValueWrapper) aggregatedValue;
      aggregateObj.put(aggregatedValueWrapper.getKey(), aggregatedValueWrapper.getValue());
    }
    scenarioObj.put("aggregators", aggregateObj);
    return scenarioObj;
  }

  /*
   * Converts the message integrity violation wrapper to JSON.
   */
  public static JSONObject msgIntegrityToJson(
    MsgIntegrityViolationWrapper msgIntegrityViolationWrapper) throws JSONException {
    JSONObject scenarioObj = new JSONObject();
    ArrayList<JSONObject> violationsList = new ArrayList<JSONObject>();
    scenarioObj.put("superstepId", msgIntegrityViolationWrapper.getSuperstepNo());
    for (Object msgWrapper : msgIntegrityViolationWrapper.getExtendedOutgoingMessageWrappers()) {
      ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper = (ExtendedOutgoingMessageWrapper) msgWrapper;
      JSONObject violationObj = new JSONObject();
      violationObj.put("srcId", extendedOutgoingMessageWrapper .srcId);
      violationObj.put("destinationId", extendedOutgoingMessageWrapper .destinationId);
      violationObj.put("message", extendedOutgoingMessageWrapper .message);
      violationsList.add(violationObj);
    }
    scenarioObj.put("violations", violationsList);
    return scenarioObj;
  }

  /*
   * Converts the vertex integrity violation wrapper to JSON.
   */
  public static JSONObject vertexIntegrityToJson(
    GiraphVertexScenarioWrapper giraphVertexScenarioWrapper) throws JSONException {
    JSONObject scenarioObj = new JSONObject();
    VertexContextWrapper vertexContextWrapper = 
      giraphVertexScenarioWrapper.getContextWrapper();
    scenarioObj.put("vertexId", vertexContextWrapper.getVertexIdWrapper());
    scenarioObj.put("vertexValue", vertexContextWrapper.getVertexValueAfterWrapper());
    return scenarioObj;
  }

  /*
   * Returns a list of vertex Ids that were debugged in the given superstep by
   * reading (the file names of) the debug traces on HDFS. File names follow the
   * <prefix>_stp_<superstepNo>_vid_<vertexId>.tr naming convention.
   */
  public static ArrayList<String> getVerticesDebugged(String jobId, long superstepNo, 
    DebugTrace debugTrace) throws IOException {
    ArrayList<String> vertexIds = new ArrayList<String>();
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFileRoot = DebugUtils.getTraceFileRoot(jobId);
    // Use this regex to match the file name and capture the vertex id.
    String regex = String.format(DebugUtils.getTraceFileFormat(debugTrace), 
      superstepNo, "(.*?)");
    Pattern p = Pattern.compile(regex);
    Path pt = new Path(traceFileRoot);
    // Iterate through each file in this directory and match the regex.
    for (FileStatus fileStatus : fs.listStatus(pt)) {
      String fileName = new File(fileStatus.getPath().toString()).toString();
      Matcher m = p.matcher(fileName);
      // Add this vertex id if there is a match.
      if (m.find()) {
        // VERTEX_ALL debug trace has one group to match the prefix -reg|err.
        vertexIds.add(m.group(debugTrace == DebugTrace.VERTEX_ALL ? 2 : 1));
      }
    }
    return vertexIds;
  }

  /*
   * Returns the IDs of all the tasks that caused the given integrity violation.
   * @param debugTrace - Must be one of INTEGRITY_* types.
   */
  public static ArrayList<String> getTasksWithIntegrityViolations(String jobId, 
    long superstepNo, DebugTrace debugTrace) throws IOException {
    assert EnumSet.of(DebugTrace.INTEGRITY_MESSAGE_ALL, 
      DebugTrace.INTEGRITY_VERTEX).contains(debugTrace);
    ArrayList<String> taskIds = new ArrayList<String>();
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFileRoot = DebugUtils.getTraceFileRoot(jobId);
    // Use this regex to match the file name and capture the vertex id.
    String regex = String.format(DebugUtils.getTraceFileFormat(debugTrace), "(.*?)", superstepNo);
    Pattern p = Pattern.compile(regex);
    Path pt = new Path(traceFileRoot);
    // Iterate through each file in this directory and match the regex.
    for (FileStatus fileStatus : fs.listStatus(pt)) {
      String fileName = new File(fileStatus.getPath().toString()).toString();
      Matcher m = p.matcher(fileName);
      // Add this vertex id if there is a match.
      if (m.find()) {
        taskIds.add(m.group(1));
      }
    }
    return taskIds;
    
  }
  /*
   * Returns the list of supersteps for which there is an exception or
   * regular trace.
   */
  public static ArrayList<Long> getSuperstepsDebugged(String jobId) throws IOException {
      ArrayList<Long> superstepIds = new ArrayList<Long>();
      FileSystem fs = ServerUtils.getFileSystem();
      String traceFileRoot = DebugUtils.getTraceFileRoot(jobId);
      // Use this regex to match the file name and capture the vertex id.
      String regex = String.format("(reg|err)_stp_(.*?)_vid_(.*?).tr$");
      Pattern p = Pattern.compile(regex);
      Path pt = new Path(traceFileRoot);
      // Iterate through each file in this directory and match the regex.
      for (FileStatus fileStatus : fs.listStatus(pt)) {
        String fileName = new File(fileStatus.getPath().toString()).toString();
        Matcher m = p.matcher(fileName);
        // Add this vertex id if there is a match.
        if (m.find()) {
          superstepIds.add(Long.parseLong(m.group(2)));
        }
      }
      return superstepIds;
  }
}
