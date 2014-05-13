package stanford.infolab.debugger.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.File;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import stanford.infolab.debugger.Integrity.VertexValueIntegrityViolation.VertexIdValuePair;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.NeighborWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.OutgoingMessageWrapper;
import stanford.infolab.debugger.utils.MsgIntegrityViolationWrapper;
import stanford.infolab.debugger.utils.MsgIntegrityViolationWrapper.ExtendedOutgoingMessageWrapper;
import stanford.infolab.debugger.utils.VertexValueIntegrityViolationWrapper;
import stanford.infolab.debugger.utils.VertexValueIntegrityViolationWrapper.VertexIdValuePairWrapper;

import com.sun.net.httpserver.Headers;

import java.util.regex.Matcher;

/*
 * Utility methods for Debugger Server.
 */
public class ServerUtils {
  public enum DebugTrace {
    REGULAR, MESSAGE_INTEGRITY, VERTEX_INTEGRITY, EXCEPTION
  }

  public static final String JOB_ID_KEY = "jobId";
  public static final String VERTEX_ID_KEY = "vertexId";
  public static final String SUPERSTEP_ID_KEY = "superstepId";
  public static final String INTEGRITY_VIOLATION_TYPE_KEY = "type";

  private static final String TRACE_ROOT = "";

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
   */
  public static FileSystem getFileSystem() throws IOException {
    String coreSitePath = "/usr/local/hadoop/conf/core-site.xml";
    Configuration configuration = new Configuration();
    configuration.addResource(new Path(coreSitePath));
    return FileSystem.get(configuration);
  }

  /*
   * Returns the file name of the trace file given the three parameters. Pass
   * arbitrary vertexId for traces which do not require a vertexId.
   */
  public static String getTraceFileName(long superstepNo, DebugTrace debugTrace, String optVertexId) {
    switch (debugTrace) {
    case REGULAR:
      return String.format("reg_stp_%d_vid_%s.tr", superstepNo, optVertexId);
    case MESSAGE_INTEGRITY:
      return String.format("msg_intgrty_stp_%d.tr", superstepNo);
    case VERTEX_INTEGRITY:
      return String.format("vv_intgrty_stp_%d.tr", superstepNo);
    case EXCEPTION:
      return String.format("err_stp_%d_vid_%s.tr", superstepNo, optVertexId);
    default:
      throw new IllegalArgumentException("DebugTrace not supported.");
    }
  }

  /*
   * Returns the root directory of the trace file. For instance
   * /giraph-traces/job_40/reg_xx_tr.tr Returns /giraph-traces/job_40
   */
  public static String getTraceFileRoot(String jobId, DebugTrace debugTrace) {
    switch (debugTrace) {
    case REGULAR:
      return String.format("%s/%s", ServerUtils.TRACE_ROOT, jobId);
    case MESSAGE_INTEGRITY:
      return String.format("%s/%s/integrity_traces", ServerUtils.TRACE_ROOT, jobId);
    case VERTEX_INTEGRITY:
      return String.format("%s/%s/integrity_traces", ServerUtils.TRACE_ROOT, jobId);
    case EXCEPTION:
      return String.format("%s/%s", ServerUtils.TRACE_ROOT, jobId);
    default:
      throw new IllegalArgumentException("DebugTrace not supported.");
    }
  }

  /*
   * Returns the path of the trace file on HDFS.
   */
  public static String getTraceFilePath(String jobId, long superstepNo, DebugTrace debugTrace, 
    String optVertexId) {
    return String.format("%s/%s",
        ServerUtils.getTraceFileRoot(jobId, debugTrace),
        ServerUtils.getTraceFileName(superstepNo, debugTrace, optVertexId));
  }

  /*
   * Reads the protocol buffer trace corresponding to the given jobId,
   * superstepNo and vertexId and returns the giraphScenarioWrapper.
   * 
   * @param jobId : ID of the job debugged.
   * 
   * @param superstepNo: Superstep number debugged.
   * 
   * @param vertexId - ID of the vertex debugged. Returns GiraphScenarioWrapper.
   */
  public static GiraphScenarioWrapper readScenarioFromTrace(String jobId, long superstepNo,
    String vertexId) throws IOException, ClassNotFoundException, InstantiationException,
    IllegalAccessException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo,
      DebugTrace.REGULAR, vertexId);
    GiraphScenarioWrapper giraphScenarioWrapper = new GiraphScenarioWrapper();
    giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath);
    return giraphScenarioWrapper;
  }

  /*
   * Returns the MessageIntegrityViolationWrapper from trace file.
   */
  public static MsgIntegrityViolationWrapper readMsgIntegrityViolationFromTrace(String jobId,
    long superstepNo) throws IOException, ClassNotFoundException, InstantiationException,
    IllegalAccessException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo, 
      DebugTrace.MESSAGE_INTEGRITY, null /* message integrity does not require vertexId */);
    MsgIntegrityViolationWrapper msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper();
    msgIntegrityViolationWrapper.loadFromHDFS(fs, traceFilePath);
    return msgIntegrityViolationWrapper;
  }

  /*
   * Returns the MessageIntegrityViolationWrapper from trace file.
   */
  public static VertexValueIntegrityViolationWrapper readVertexIntegrityViolationFromTrace(
    String jobId, long superstepNo) throws IOException, ClassNotFoundException,
    InstantiationException, IllegalAccessException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo, 
      DebugTrace.VERTEX_INTEGRITY, null /* vertex integrity does not require vertexId */);
    VertexValueIntegrityViolationWrapper vertexValueIntegrityViolationWrapper = new VertexValueIntegrityViolationWrapper();
    vertexValueIntegrityViolationWrapper.loadFromHDFS(fs, traceFilePath);
    return vertexValueIntegrityViolationWrapper;
  }

  /*
   * Returns the MessageIntegrityViolationWrapper from trace file.
   */
  public static GiraphScenarioWrapper readExceptionFromTrace(String jobId, long superstepNo,
    String vertexId) throws IOException, ClassNotFoundException, InstantiationException,
    IllegalAccessException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo,
      DebugTrace.EXCEPTION, vertexId);
    GiraphScenarioWrapper giraphScenarioWrapper = new GiraphScenarioWrapper();
    giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath);
    return giraphScenarioWrapper;
  }

  /*
   * Returns the raw bytes of the debug trace file.
   */
  public static byte[] readTrace(String jobId, long superstepNo, String vertexId)
    throws IOException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo,
      DebugTrace.REGULAR, vertexId);
    byte[] data = IOUtils.toByteArray(fs.open(new Path(traceFilePath)));
    return data;
  }

  /*
   * Converts a Giraph Scenario (giraphScenarioWrapper object) to JSON
   * (JSONObject)
   * @param giraphScenarioWrapper : Giraph Scenario object.
   */
  public static JSONObject scenarioToJSON(GiraphScenarioWrapper giraphScenarioWrapper)
    throws JSONException {
    ContextWrapper contextWrapper = giraphScenarioWrapper.getContextWrapper();
    JSONObject scenarioObj = new JSONObject();
    scenarioObj.put("vertexId", contextWrapper.getVertexIdWrapper());
    scenarioObj.put("vertexValue", contextWrapper.getVertexValueAfterWrapper());
    JSONObject outgoingMessagesObj = new JSONObject();
    ArrayList<String> neighborsList = new ArrayList<String>();
    // Add outgoing messages.
    for (Object outgoingMessage : contextWrapper.getOutgoingMessageWrappers()) {
      OutgoingMessageWrapper outgoingMessageWrapper = (OutgoingMessageWrapper) outgoingMessage;
      outgoingMessagesObj.put(outgoingMessageWrapper.destinationId.toString(),
        outgoingMessageWrapper.message.toString());
    }
    // Add neighbors.
    for (Object neighbor : contextWrapper.getNeighborWrappers()) {
      NeighborWrapper neighborWrapper = (NeighborWrapper) neighbor;
      neighborsList.add(neighborWrapper.getNbrId().toString());
    }
    scenarioObj.put("outgoingMessages", outgoingMessagesObj);
    scenarioObj.put("neighbors", neighborsList);
    // Add exception, if present.
    if (giraphScenarioWrapper.hasExceptionWrapper()) {
      JSONObject exceptionObj = new JSONObject();
      GiraphScenarioWrapper.ExceptionWrapper exceptionWrapper = giraphScenarioWrapper
        .getExceptionWrapper();
      exceptionObj.put("message", exceptionWrapper.getErrorMessage());
      exceptionObj.put("stackTrace", exceptionWrapper.getStackTrace());
      scenarioObj.put("exception", exceptionObj);
    }
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
    VertexValueIntegrityViolationWrapper vertexValueIntegrityViolationWrapper) throws JSONException {
    JSONObject scenarioObj = new JSONObject();
    ArrayList<JSONObject> violationsList = new ArrayList<JSONObject>();
    scenarioObj.put("superstepId", vertexValueIntegrityViolationWrapper.getSuperstepNo());
    for (Object pair : vertexValueIntegrityViolationWrapper.getVertexIdValuePairWrappers()) {
      VertexIdValuePairWrapper vertexIdValuePair = (VertexIdValuePairWrapper) pair;
      JSONObject violationObj = new JSONObject();
      violationObj.put("vertexId", vertexIdValuePair.vertexId);
      violationObj.put("vertexValue", vertexIdValuePair.vertexValue);
      violationsList.add(violationObj);
    }
    scenarioObj.put("violations", violationsList);
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
    String traceFileRoot = ServerUtils.getTraceFileRoot(jobId, debugTrace);
    // Use this regex to match the file name and capture the vertex id.
    String prefix = "reg";
    if (debugTrace == DebugTrace.EXCEPTION) {
      prefix = "err";
    }
    String regex = String.format("%s_stp_%d_vid_(.*?).tr$", prefix, superstepNo);
    Pattern p = Pattern.compile(regex);
    Path pt = new Path(traceFileRoot);
    // Iterate through each file in this directory and match the regex.
    for (FileStatus fileStatus : fs.listStatus(pt)) {
      String fileName = new File(fileStatus.getPath().toString()).toString();
      Matcher m = p.matcher(fileName);
      // Add this vertex id if there is a match.
      if (m.find()) {
        vertexIds.add(m.group(1));
      }
    }
    return vertexIds;
  }
  
  /*
   * Returns the list of supersteps for which there is an exception or
   * regular trace.
   */
  public static ArrayList<Long> getSuperstepsDebugged(String jobId) throws IOException {
      ArrayList<Long> superstepIds = new ArrayList<Long>();
      FileSystem fs = ServerUtils.getFileSystem();
      String traceFileRoot = ServerUtils.getTraceFileRoot(jobId, DebugTrace.REGULAR);
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
