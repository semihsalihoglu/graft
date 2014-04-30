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
 * The Abstract class for HTTP handlers. 
 */
public abstract class ServerHttpHandler implements HttpHandler {
    // Response body.
    protected String response;
    // Response status code. Please use HttpUrlConnection final static members.
    protected int statusCode;

    /* 
     * Handles an HTTP call's lifecycle - read parameters, process and send response.
     */
    public void handle(HttpExchange httpExchange) throws IOException {
        String rawUrl = httpExchange.getRequestURI().getQuery();
        HashMap<String, String> paramMap;
        int statusCode;

        try {
            paramMap = ServerUtils.getUrlParams(rawUrl);
            // Call the method implemeneted by inherited classes.
            processRequest(paramMap);
        }
        catch(UnsupportedEncodingException ex) {
            this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST; 
            this.response = "Malformed URL. Given encoding is not supported.";
        }

        // Send response.
        ServerUtils.setMandatoryResponseHeaders(httpExchange.getResponseHeaders());
        httpExchange.sendResponseHeaders(this.statusCode, this.response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(this.response.getBytes());
        os.close();
    }

    /*
     * Implement this method in inherited classes. 
     * This method MUST set statusCode and response class members appropriately.
     */
    public abstract void processRequest(HashMap<String, String> paramMap); 
}
