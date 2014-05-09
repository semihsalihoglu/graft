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

import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.NeighborWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.OutgoingMessageWrapper;

import com.sun.net.httpserver.Headers;
import java.util.regex.Matcher;

/*
 * Utility methods for Debugger Server.
 */
public class ServerUtils {
  public static final String JOB_ID_KEY = "jobId";
  public static final String VERTEX_ID_KEY = "vertexId";
  public static final String SUPERSTEP_ID_KEY = "superstepId";
  
  private static final String TRACE_ROOT = "";

  /*
   * Returns parameters of the URL in a hash map. For instance,
   * http://localhost:9000/?key1=val1&key2=val2&key3=val3
   */
  public static HashMap<String, String> getUrlParams(String rawUrl)
    throws UnsupportedEncodingException {
    HashMap<String, String> paramMap = new HashMap<String, String>();
    String[] params = rawUrl.split("&");
    for (String param : params) {
      String[] parts = param.split("=");
      String paramKey = URLDecoder.decode(parts[0], "UTF-8");
      String paramValue = URLDecoder.decode(parts[1], "UTF-8");
      paramMap.put(paramKey, paramValue);
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
   * Returns the file name of the trace file given the three parameters.
   */
  public static String getTraceFileName(long superstepNo, String vertexId) {
    return String.format("reg_stp_%d_vid_%s.tr", superstepNo, vertexId);
  }
  
  /*
   * Returns the root directory of the trace file. 
   * For instance /giraph-traces/job_40/reg_xx_tr.tr
   * Returns /giraph-traces/job_40
   */
  public static String getTraceFileRoot(String jobId, long superstepNo) {
    return String.format("%s/%s", ServerUtils.TRACE_ROOT, jobId);
  }
  
  public static String getTraceFilePath(String jobId, long superstepNo, String vertexId) {
    return String.format("%s/%s", ServerUtils.getTraceFileRoot(jobId, superstepNo),
      ServerUtils.getTraceFileName(superstepNo, vertexId));
  }
  
  /*
   * Reads the protocol buffer trace corresponding to the given jobId,
   * superstepNo and vertexId and returns the giraphScenarioWrapper.
   * @param jobId : ID of the job debugged.
   * @param superstepNo: Superstep number debugged.
   * @param vertexId - ID of the vertex debugged. Returns GiraphScenarioWrapper.
   */
  public static GiraphScenarioWrapper readScenarioFromTrace(String jobId, long superstepNo,
    String vertexId) throws IOException, ClassNotFoundException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo, vertexId);
    GiraphScenarioWrapper giraphScenarioWrapper = new GiraphScenarioWrapper();
    giraphScenarioWrapper.loadFromHDFS(fs, traceFilePath);
    return giraphScenarioWrapper;
  }

  public static byte[] readTrace(String jobId, long superstepNo, String vertexId)
    throws IOException {
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFilePath = ServerUtils.getTraceFilePath(jobId, superstepNo, vertexId);
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
    for (Object outgoingMessage : contextWrapper.getOutgoingMessageWrappers()) {
      OutgoingMessageWrapper outgoingMessageWrapper = (OutgoingMessageWrapper) outgoingMessage;
      outgoingMessagesObj.put(outgoingMessageWrapper.destinationId.toString(),
        outgoingMessageWrapper.message.toString());
    }
    for (Object neighbor : contextWrapper.getNeighborWrappers()) {
      NeighborWrapper neighborWrapper = (NeighborWrapper) neighbor;
      neighborsList.add(neighborWrapper.getNbrId().toString());
    }
    scenarioObj.put("outgoingMessages", outgoingMessagesObj);
    scenarioObj.put("neighbors", neighborsList);
    return scenarioObj;
  }

  /*
   * Returns a list of vertex Ids that were debugged in the given superstep by
   * reading (the file names of) the debug traces on HDFS. File names follow the
   * tr_stp_<superstepNo>_vid_<vertexId>.tr naming convention.
   */
  public static ArrayList<String> getVerticesDebugged(String jobId, long superstepNo)
    throws IOException {
    ArrayList<String> vertexIds = new ArrayList<String>();
    FileSystem fs = ServerUtils.getFileSystem();
    String traceFileRoot = ServerUtils.getTraceFileRoot(jobId, superstepNo);
    // Use this regex to match the file name and capture the vertex id.
    String regex = String.format("reg_stp_%d_vid_(.*?).tr$", superstepNo);
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
}
