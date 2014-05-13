package stanford.infolab.debugger.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.io.UnsupportedEncodingException;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.eclipse.jdt.core.dom.ThisExpression;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import stanford.infolab.debugger.server.ServerUtils.DebugTrace;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.NeighborWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.OutgoingMessageWrapper;
import stanford.infolab.debugger.utils.MsgIntegrityViolationWrapper;
import stanford.infolab.debugger.utils.VertexValueIntegrityViolationWrapper;
import sun.security.ssl.Debug;

import com.google.common.net.HttpHeaders;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/*
 * Entry point to the HTTP Debugger Server. 
 */
public class Server {
  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
    // Attach JobHandler instance to handle /job GET call.
    server.createContext("/job", new GetJob());
    server.createContext("/vertices", new GetVertices());
    server.createContext("/supersteps", new GetSupersteps());
    server.createContext("/scenario", new GetScenario());
    server.createContext("/integrity", new GetIntegrity());
    // Creates a default executor.
    server.setExecutor(null);
    server.start();
  }

  /*
   * Handles /job HTTP GET call. Returns the details of the given jobId.
   * @URLparams -{jobId}
   */
  static class GetJob extends ServerHttpHandler {
    public void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      Debug.println("/job", paramMap.toString());
      if (jobId != null) {
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.response = getSuperstepData(jobId);
      } else {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("Invalid parameters. %s is mandatory parameter.",
          ServerUtils.JOB_ID_KEY);
      }
    }

    /*
     * Returns superstep data of the job in JSON format. TODO(vikesh):
     * Sample/Demo method for now. Will remove after modifying the front-end
     * with the new API.
     */
    private String getSuperstepData(String joId) {
      return "[{'PR1' : {adj: ['PR2', 'PR3'], attrs:[0.244], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'PR3' : 'msgFrom1To3.step-1'}}, 'PR2' : {adj: ['PR3'], attrs:    [0.455], msgs: {'PR3': 'msgTo3From2.step-1'}}, 'PR4' : {adj: ['PR1'], attrs:[0.78]}},  {'PR1' : {attrs:[0.448], msgs:{ 'PR2' : 'msgFrom1To2.step0', 'PR3' : 'msgFrom    1To3.step0'}}, 'PR2' : {attrs:[0.889], msgs: {'PR3': 'msgTo3From2.step0'}}, 'PR4' : {attrs:[0.98]}}, {'PR1' : {attrs:[0.001], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'P    R3' : 'msgFrom1To3.step1'}}, 'PR2' : {attrs:[0.667], msgs: {'PR3': 'msgTo3From2.step1'}}}, {'PR1' : {attrs:[0.232], msgs:{ 'PR2' : 'msgFrom1To2.step2', 'PR3' : 'msg    From1To3.step2'}}, 'PR2' : {attrs:[0.787], msgs: {'PR3': 'msgTo3From2.step2'}}}]";
    }
  }

  /*
   * Returns the list of vertices debugged in a given Superstep for a given job.
   * @URLParams: {jobId, superstepId}
   */
  static class GetVertices extends ServerHttpHandler {
    public void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      try {
        // jobId and superstepId are mandatory. Validate.
        if (jobId == null || superstepId == null) {
          throw new IllegalArgumentException("Missing mandatory params.");
        }
        ArrayList<String> vertexIds = null;
        // May throw NumberFormatException. Handled below.
        long superstepNo = Long.parseLong(superstepId);
        if (superstepNo < -1) {
          throw new NumberFormatException("Superstep must be integer >= -1.");
        }
        // May throw IOException. Handled below.
        vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo, DebugTrace.REGULAR);
        this.statusCode = HttpURLConnection.HTTP_OK;
        // Returns output as an array ["id1", "id2", "id3" .... ]
        this.response = new JSONArray(vertexIds).toString();
      } catch (NumberFormatException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("%s must be an integer >= -1.", ServerUtils.SUPERSTEP_ID_KEY);
      } catch (IllegalArgumentException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("Invalid parameters. %s is a mandatory parameter.",
          ServerUtils.JOB_ID_KEY);
      } catch (IOException e) {
        // IOException is unexpected in this case. Return Internal Server Error.
        this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        this.response = "Internal Server Error.";
      }
    }
  }
  
  /*
   * Returns the number of supersteps traced for the given job.
   */
  static class GetSupersteps extends ServerHttpHandler {
    public void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      try {
        // jobId and superstepId are mandatory. Validate.
        if (jobId == null) {
          throw new IllegalArgumentException("Missing mandatory params.");
        }
        ArrayList<Long> superstepIds = null;
        // May throw IOException. Handled below.
        superstepIds = ServerUtils.getSuperstepsDebugged(jobId);
        this.statusCode = HttpURLConnection.HTTP_OK;
        // Returns output as an array ["id1", "id2", "id3" .... ]
        this.response = new JSONArray(superstepIds).toString();
      } catch (NumberFormatException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("%s must be an integer >= -1.", ServerUtils.SUPERSTEP_ID_KEY);
      } catch (IllegalArgumentException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("Invalid parameters. %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY);
      } catch (IOException e) {
        // IOException is unexpected in this case. Return Internal Server Error.
        this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        this.response = "Internal Server Error.";
      }
    }
 }

  /*
   * Returns the scenario for a given superstep of a given job.
   * @URLParams - {jobId, superstepId, [vertexId], [raw]}
   * @desc vertexId - vertexId is optional. It can be a single value or a comma
   * separated list. If it is not supplied, returns the scenario for all
   * vertices. If 'raw' parameter is specified, returns the raw protocol buffer.
   */
  static class GetScenario extends ServerHttpHandler {
    public void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      Debug.println("/scenario", paramMap.toString());
      // Check both jobId and superstepId are present
      try {
        if (jobId == null || superstepId == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }
        Long superstepNo = Long.parseLong(paramMap.get(ServerUtils.SUPERSTEP_ID_KEY));
        if (superstepNo < -1) {
          this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
          this.response = String.format("%s must be an integer >= -1.",
            ServerUtils.SUPERSTEP_ID_KEY);
          return;
        }
        ArrayList<String> vertexIds = null;
        // Get the single vertexId or the list of vertexIds (comma-separated).
        String rawVertexIds = paramMap.get(ServerUtils.VERTEX_ID_KEY);
        // No vertex Id supplied. Return scenario for all vertices.
        if (rawVertexIds == null) {
          // Read scenario for all vertices.
          // May throw IOException. Handled below.
          vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo, DebugTrace.REGULAR);
        } else {
          // Split the vertices by comma.
          vertexIds = new ArrayList(Arrays.asList(rawVertexIds.split(",")));
        }
        // Check if raw protocol buffers were requested.
        if (paramMap.get("raw") != null) {
          if (vertexIds.size() > 1) {
            this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
            this.response = "Raw protocol Buffers may only be returned with a single vertex.";
            return;
          }
          String vertexId = vertexIds.get(0).trim();
          this.responseContentType = MediaType.APPLICATION_OCTET_STREAM;
          this.statusCode = HttpURLConnection.HTTP_OK;
          this.responseBytes = ServerUtils.readTrace(jobId, superstepNo, vertexId);
          // Set this header to force a download with the given filename.
          String fileName = String.format("%s_%s", jobId, 
            ServerUtils.getTraceFileName(superstepNo, ServerUtils.DebugTrace.REGULAR, vertexId));
          this.setResponseHeader("Content-disposition", "attachment; filename=" + fileName);
          return;
        }
        // Send JSON by default.
        JSONObject scenarioObj = new JSONObject();
        for (String vertexId : vertexIds) {
          GiraphScenarioWrapper giraphScenarioWrapper;
          giraphScenarioWrapper = ServerUtils.readScenarioFromTrace(jobId, superstepNo,
            vertexId.trim());
          scenarioObj.put(vertexId, ServerUtils.scenarioToJSON(giraphScenarioWrapper));
        }
        // Set status as OK and convert JSONObject to string.
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.response = scenarioObj.toString();
      } catch (IllegalArgumentException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("Invalid parameters. %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY);
      } catch (ClassNotFoundException|JSONException e) {
        this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        this.response = "Internal Server Error";
      } catch (IOException|InstantiationException|IllegalAccessException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = "Could not read the debug trace for this vertex.";
      }
    }
  }
  
  /*
   * Returns the integrity violations based on the requested parameter.
   * The requested parameter (type) may be one of M, E or V.
   */
  static class GetIntegrity extends ServerHttpHandler {
    public void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      String violationType = paramMap.get(ServerUtils.INTEGRITY_VIOLATION_TYPE_KEY);
      Debug.println("/integrity", paramMap.toString());
      // Check both jobId and superstepId are present
      try {
        if (jobId == null || superstepId == null || violationType == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }  
        Long superstepNo = Long.parseLong(paramMap.get(ServerUtils.SUPERSTEP_ID_KEY));
        if (superstepNo < -1) {
          this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
          this.response = String.format("%s must be an integer >= -1.",
            ServerUtils.SUPERSTEP_ID_KEY);
          return;
        }
        // Message violation
        if(violationType.equals("M")) {
          MsgIntegrityViolationWrapper msgIntegrityViolationWrapper = 
            ServerUtils.readMsgIntegrityViolationFromTrace(jobId, superstepNo);
          JSONObject scenarioObj = ServerUtils.msgIntegrityToJson(msgIntegrityViolationWrapper);
          this.response = scenarioObj.toString();
          this.statusCode = this.statusCode = HttpURLConnection.HTTP_OK;
        } else if(violationType.equals("V")) {
          VertexValueIntegrityViolationWrapper vertexValueIntegrityViolationWrapper =
            ServerUtils.readVertexIntegrityViolationFromTrace(jobId, superstepNo);
          JSONObject scenarioObj = ServerUtils.vertexIntegrityToJson(
            vertexValueIntegrityViolationWrapper);
          this.response = scenarioObj.toString();
          this.statusCode = this.statusCode = HttpURLConnection.HTTP_OK;
        } else if(violationType.equals("E")) {
          ArrayList<String> vertexIds = null;
          // Get the single vertexId or the list of vertexIds (comma-separated).
          String rawVertexIds = paramMap.get(ServerUtils.VERTEX_ID_KEY);
          // No vertex Id supplied. Return exceptions for all vertices.
          if (rawVertexIds == null) {
            // Read exceptions for all vertices.
            vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo, DebugTrace.EXCEPTION);
          } else {
            // Split the vertices by comma.
            vertexIds = new ArrayList(Arrays.asList(rawVertexIds.split(",")));
          }
          // Send JSON by default.
          JSONObject scenarioObj = new JSONObject();
          for (String vertexId : vertexIds) {
            GiraphScenarioWrapper giraphScenarioWrapper;
            giraphScenarioWrapper = ServerUtils.readExceptionFromTrace(jobId, superstepNo,
              vertexId.trim());
            scenarioObj.put(vertexId, ServerUtils.scenarioToJSON(giraphScenarioWrapper));
          }
          // Set status as OK and convert JSONObject to string.
          this.statusCode = HttpURLConnection.HTTP_OK;
          this.response = scenarioObj.toString();
        }
      } catch (IllegalArgumentException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = String.format("Invalid parameters. %s, %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY, 
          ServerUtils.INTEGRITY_VIOLATION_TYPE_KEY);
      } catch(FileNotFoundException e) {
        // If file is not found, send an empty OK response.
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.response = new JSONObject().toString();
      } catch (IOException|InstantiationException|IllegalAccessException e) {
        this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
        this.response = "Could not read the debug trace for this vertex.";
      } catch (ClassNotFoundException|JSONException e) {
        this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        this.response = "Internal Server Error";
      }
    }
  }
}