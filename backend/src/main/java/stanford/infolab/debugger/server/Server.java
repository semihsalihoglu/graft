package stanford.infolab.debugger.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.HttpURLConnection;

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
        server.createContext("/job", new JobHandler());
        // Creates a default executor.
        server.setExecutor(null); 
        server.start();
    }

    /*
     * Handles /job HTTP GET call. 
     * Returns the details of the given job_id. 
     */
    static class JobHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String rawQuery = t.getRequestURI().getQuery();
            String jobId = null;
        
            // General URL format is: ?key1=val1&key2=val2.
            // /job format is: ?job_id=value. Other parameters are ignored.
            String[] params = rawQuery.split("&");
            for (String param : params) {
                String[] parts = param.split("=");
                String key = URLDecoder.decode(parts[0], "UTF-8");
                if (key.equals("job_id")) {
                    jobId = URLDecoder.decode(parts[1], "UTF-8");
                }
            }

            String response;
            int statusCode;
            // job_id parameter not found - bad request.
            if (jobId == null) {
                statusCode = HttpURLConnection.HTTP_BAD_REQUEST; 
                response = "Invalid parameters. job_id is a required parameter";
            }
            else {
                statusCode = HttpURLConnection.HTTP_OK;
                response = GetSuperstepData(jobId);
            }
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
}
