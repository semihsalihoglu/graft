package stanford.infolab.debugger.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.io.UnsupportedEncodingException;
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
    static class GetJob implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String rawUrl = t.getRequestURI().getQuery();
            HashMap<String, String> paramMap;
            String jobId, response;
            int statusCode;

            try {
                paramMap = ServerUtils.getUrlParams(rawUrl);
                jobId = paramMap.get(ServerUtils.JOB_ID_KEY);

                if (jobId != null) {
                    statusCode = HttpURLConnection.HTTP_OK;
                    response = GetSuperstepData(jobId);
                } else {
                    statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
                    response = String.format("Invalid parameters. %s is mandatory parameter.",
                            ServerUtils.JOB_ID_KEY);;
                }
            }
            catch(UnsupportedEncodingException ex) {
                statusCode = HttpURLConnection.HTTP_BAD_REQUEST; 
                response = "Malformed URL. Given encoding is not supported.";
            }
            ServerUtils.setMandatoryResponseHeaders(t.getResponseHeaders());
            t.sendResponseHeaders(statusCode, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

        /*
         * Returns superstep data of the job in JSON format.
         * TODO(vikesh): Sample/Demo method for now. Use a proper package to encode JSON.
         */
        private String GetSuperstepData(String job_id) {
                return  "[{'PR1' : {adj: ['PR2', 'PR3'], attrs:[0.244], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'PR3' : 'msgFrom1To3.step-1'}}, 'PR2' : {adj: ['PR3'], attrs:    [0.455], msgs: {'PR3': 'msgTo3From2.step-1'}}, 'PR4' : {adj: ['PR1'], attrs:[0.78]}},  {'PR1' : {attrs:[0.448], msgs:{ 'PR2' : 'msgFrom1To2.step0', 'PR3' : 'msgFrom    1To3.step0'}}, 'PR2' : {attrs:[0.889], msgs: {'PR3': 'msgTo3From2.step0'}}, 'PR4' : {attrs:[0.98]}}, {'PR1' : {attrs:[0.001], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'P    R3' : 'msgFrom1To3.step1'}}, 'PR2' : {attrs:[0.667], msgs: {'PR3': 'msgTo3From2.step1'}}}, {'PR1' : {attrs:[0.232], msgs:{ 'PR2' : 'msgFrom1To2.step2', 'PR3' : 'msg    From1To3.step2'}}, 'PR2' : {attrs:[0.787], msgs: {'PR3': 'msgTo3From2.step2'}}}]";
        }
    }

    /*
     * Returns the list of vertices debugged in a given superstep for a given job. 
     * @URLParams: {jobId, superstepId}
     */
    static class GetVertices implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String rawUrl = t.getRequestURI().getQuery();
            HashMap<String, String> paramMap;
            String jobId, superstepId, response;
            int statusCode;

            try {
                paramMap = ServerUtils.getUrlParams(rawUrl);
                jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
                superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);

                // TODO(vikesh): Replace with actual data.
                if (jobId != null && superstepId != null) {
                    statusCode = HttpURLConnection.HTTP_OK;
                    response = "['vertexId1', 'vertexId2', 'vertexId3', 'vertexId4']";
                } else {
                    statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
                    response = String.format("Invalid parameters. %s and %s are mandatory parameter.",
                            ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY);
                }
            }
            catch(UnsupportedEncodingException ex) {
                statusCode = HttpURLConnection.HTTP_BAD_REQUEST; 
                response = "Malformed URL. Given encoding is not supported.";
            }
            ServerUtils.setMandatoryResponseHeaders(t.getResponseHeaders());
            t.sendResponseHeaders(statusCode, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    /*
     * Returns the scenario for a given superstep of a given job.
     * @URLParams - {jobId, superstepId, [vertexId]}
     * @desc vertexId - vertexId is optional. It can be a single value 
     * or a comma separated list. If it is not supplied, returns the scenario 
     * for all vertices.
     */
    static class GetScenario implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String rawUrl = t.getRequestURI().getQuery();
            HashMap<String, String> paramMap;
            String jobId, superstepId, response;
            int statusCode;

            try {
                paramMap = ServerUtils.getUrlParams(rawUrl);
                jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
                superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);

                // TODO(vikesh): Replace with actual data and handle multiple/single vertices properly.
                if (jobId != null && superstepId != null) {
                    statusCode = HttpURLConnection.HTTP_OK;

                    // Get the single vertexId or the list of vertexIds (comma-separated).
                    String rawVertexIds = paramMap.get(ServerUtils.VERTEX_ID_KEY);

                    // No vertex Id supplied. Return scenario for all vertices.
                    if (rawVertexIds == null) {
                        //TODO(vikesh) : Read scenario for all vertices.
                    }
                    else {
                        String[] vertexIds = rawVertexIds.split(",");

                        for(String vertexId : vertexIds) {
                            // TODO(vikesh) : Read scenario for each vertex in the input.
                        }
                    }
                    response = "{'vertexId1' : {}, 'vertexId2' : {}}";
                } else {
                    statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
                    response = String.format("Invalid parameters. %s and %s are mandatory parameter.", 
                            ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY);
                }
            }
            catch(UnsupportedEncodingException ex) {
                statusCode = HttpURLConnection.HTTP_BAD_REQUEST; 
                response = "Malformed URL. Given encoding is not supported.";
            }
            ServerUtils.setMandatoryResponseHeaders(t.getResponseHeaders());
            t.sendResponseHeaders(statusCode, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
