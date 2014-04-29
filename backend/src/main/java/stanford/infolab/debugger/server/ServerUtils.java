package stanford.infolab.debugger.server;

import java.util.HashMap;
import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;
import com.sun.net.httpserver.Headers;

/*
 * Utility methods for Debugger Server.
 */
public class ServerUtils {
    public static final String JOB_ID_KEY = "jobId";
    public static final String VERTEX_ID_KEY = "vertexId";
    public static final String SUPERSTEP_ID_KEY = "superstepId";

    /*
     * Returns parameters of the URL in a hash map. For instance,
     * http://localhost:9000/?key1=val1&key2=val2&key3=val3
     */
    public static HashMap<String, String> getUrlParams(String rawUrl) throws UnsupportedEncodingException {
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
     * Add mandatory headers to the HTTP response by the debugger server.
     * MUST be called before sendResponseHeaders.
     */
    public static void setMandatoryResponseHeaders(Headers headers) {
        // TODO(vikesh): **REMOVE CORS FOR ALL AFTER DECIDING THE DEPLOYMENT ENVIRONMENT**
        headers.add("Access-Control-Allow-Origin", "*");
    }
}
