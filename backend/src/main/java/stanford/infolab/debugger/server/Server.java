package stanford.infolab.debugger.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.NeighborWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.OutgoingMessageWrapper;

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
        server.createContext("/scenario", new GetScenario());
        // Creates a default executor.
        server.setExecutor(null);
        server.start();
    }

    /*
     * Handles /job HTTP GET call. 
     * Returns the details of the given jobId
     * @URLparams -{jobId}
     */
    static class GetJob extends ServerHttpHandler {
        public void processRequest(HashMap<String, String> paramMap) {
            String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
            if (jobId != null) {
                this.statusCode = HttpURLConnection.HTTP_OK;
                this.response = GetSuperstepData(jobId);
            } else {
                this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
                this.response = String.format("Invalid parameters. %s is mandatory parameter.",
                    ServerUtils.JOB_ID_KEY);;
            }
        }

        /*
         * Returns superstep data of the job in JSON format.
         * TODO(vikesh): Sample/Demo method for now. Use a proper package to encode JSON.
         */
        private String GetSuperstepData(String joId) {
                return  "[{'PR1' : {adj: ['PR2', 'PR3'], attrs:[0.244], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'PR3' : 'msgFrom1To3.step-1'}}, 'PR2' : {adj: ['PR3'], attrs:    [0.455], msgs: {'PR3': 'msgTo3From2.step-1'}}, 'PR4' : {adj: ['PR1'], attrs:[0.78]}},  {'PR1' : {attrs:[0.448], msgs:{ 'PR2' : 'msgFrom1To2.step0', 'PR3' : 'msgFrom    1To3.step0'}}, 'PR2' : {attrs:[0.889], msgs: {'PR3': 'msgTo3From2.step0'}}, 'PR4' : {attrs:[0.98]}}, {'PR1' : {attrs:[0.001], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'P    R3' : 'msgFrom1To3.step1'}}, 'PR2' : {attrs:[0.667], msgs: {'PR3': 'msgTo3From2.step1'}}}, {'PR1' : {attrs:[0.232], msgs:{ 'PR2' : 'msgFrom1To2.step2', 'PR3' : 'msg    From1To3.step2'}}, 'PR2' : {attrs:[0.787], msgs: {'PR3': 'msgTo3From2.step2'}}}]";
        }
    }

    /*
     * Returns the list of vertices debugged in a given Superstep for a given job. 
     * @URLParams: {jobId, superstepId}
     */
    static class GetVertices extends ServerHttpHandler {
        public void processRequest(HashMap<String, String> paramMap) {
            String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
            String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
            
            // jobId and superstepId are mandatory. Validate.
            if (jobId == null || superstepId == null ) {
            	 this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
                 this.response = String.format("Invalid parameters. %s and %s are mandatory parameter.",
                         ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY);
                 return;
            }
        	ArrayList<String> vertexIds = null;
        	try {
        		vertexIds = ServerUtils.getVerticesDebugged("job_201405061024_0019", 0);
        	}
        	catch(IOException ex) {
        		// IOException is unexpected in this case. Return Internal Server Error.
        		this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        		this.response = "Internal Server Error.";
        		return;
        	}
            this.statusCode = HttpURLConnection.HTTP_OK;
            // Returns output as an array ["id1", "id2", "id3" .... ]
            this.response = new JSONArray(vertexIds).toString();
        }
    }

    /*
     * Returns the scenario for a given superstep of a given job.
     * @URLParams - {jobId, superstepId, [vertexId]}
     * @desc vertexId - vertexId is optional. It can be a single value 
     * or a comma separated list. If it is not supplied, returns the scenario 
     * for all vertices.
     */
    static class GetScenario extends ServerHttpHandler {
        public void processRequest(HashMap<String, String> paramMap) {
            String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
            String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
            // Check both jobId and superstepId are present
            if (jobId == null || superstepId == null) {
            	this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
            	this.response = String.format("Invalid parameters. %s and %s are mandatory parameter.", 
                        ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY);
                return;
            }
            Long superstepNo;
            // Parse integral value of superstep from parameter. 
            try { 
            	superstepNo = Long.parseLong(paramMap.get(ServerUtils.SUPERSTEP_ID_KEY));
            	
            	if (superstepNo < -1) {
            		throw new NumberFormatException("Superstep must be >= -1");
            	}
            }
            catch (NumberFormatException ex) {
                this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
                this.response = String.format("%s must be an integer >= -1.", 
            			ServerUtils.SUPERSTEP_ID_KEY);
                return;
            }
            
            JSONObject scenarioObj = new JSONObject();
            ArrayList<String> vertexIds = null;
           	// Get the single vertexId or the list of vertexIds (comma-separated).
            String rawVertexIds = paramMap.get(ServerUtils.VERTEX_ID_KEY);
      
            // No vertex Id supplied. Return scenario for all vertices.
            if (rawVertexIds == null) {
                // Read scenario for all vertices. 
            	try {
            		vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo);
            	}
            	catch(IOException ex) {
            		this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
            		this.response = "Internal Server Error";
            		return;
            	}
            }
            else {
            	// Split the vertices by comma.
                vertexIds = new ArrayList(Arrays.asList(rawVertexIds.split(",")));
            }
            
            // For each vertex, read the scenario and add to the JSON object.
            for(String vertexId : vertexIds) {
            	GiraphScenarioWrapper giraphScenarioWrapper;
            	try {
            		giraphScenarioWrapper =
            				ServerUtils.readScenarioFromTrace(jobId, superstepNo, vertexId.trim());
            	}
            	catch(IOException ex) {
            		// IOException could be caused by an error on the server side
            		// or if the vertex was not debugged. 
            		this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
            		this.response = "Could not read the debug trace for this vertex. " +
            				String.format("Please ensure vertex %d was debugged in superstep %s",
            						vertexId, superstepId);
            		return;
            	}
            	catch(ClassNotFoundException ex) {
            		this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
            		this.response = "Internal Server error.";
            		return;
            	}
            	// Add this node's scenario to the final JSON
            	try {
            		scenarioObj.put(vertexId, ServerUtils.scenarioToJSON(giraphScenarioWrapper));
            	}
            	catch(JSONException ex) {
            		this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
            		this.response = "Server error.";
            		return;
            	}
            }
            // Set status as OK and convert JSONObject to string.
            this.statusCode = HttpURLConnection.HTTP_OK;
            this.response = scenarioObj.toString();
        } 
    }
}
