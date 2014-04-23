import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Server {
    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/job", new JobHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    static class JobHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String rawQuery = t.getRequestURI().getQuery();
						String response = "Invalid Parameters. job_id is a required URL parameter.";
						int statusCode = 400;
					
						String[] params = rawQuery.split("&");
						for (String param : params) {
							String[] parts = param.split("=");
							String key = URLDecoder.decode(parts[0], "UTF-8");

							if (key.equals("job_id")) {
									response = GetSuperstepData(URLDecoder.decode(parts[1], "UTF-8"));
									statusCode = 200;
							}
						}
						
            t.sendResponseHeaders(statusCode, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

				/*
				 * Returns superstep data of the job in JSON format.
				 * TODO: Sample/Demo method for now. Use a proper package to encode JSON.
				 */
				private String GetSuperstepData(String job_id) {
						return  "[{'PR1' : {adj: ['PR2', 'PR3'], attrs:[0.244], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'PR3' : 'msgFrom1To3.step-1'}}, 'PR2' : {adj: ['PR3'], attrs:    [0.455], msgs: {'PR3': 'msgTo3From2.step-1'}}, 'PR4' : {adj: ['PR1'], attrs:[0.78]}},  {'PR1' : {attrs:[0.448], msgs:{ 'PR2' : 'msgFrom1To2.step0', 'PR3' : 'msgFrom    1To3.step0'}}, 'PR2' : {attrs:[0.889], msgs: {'PR3': 'msgTo3From2.step0'}}, 'PR4' : {attrs:[0.98]}}, {'PR1' : {attrs:[0.001], msgs:{ 'PR2' : 'msgFrom1To2.step1', 'P    R3' : 'msgFrom1To3.step1'}}, 'PR2' : {attrs:[0.667], msgs: {'PR3': 'msgTo3From2.step1'}}}, {'PR1' : {attrs:[0.232], msgs:{ 'PR2' : 'msgFrom1To2.step2', 'PR3' : 'msg    From1To3.step2'}}, 'PR2' : {attrs:[0.787], msgs: {'PR3': 'msgTo3From2.step2'}}}]";
				}
    }
}
