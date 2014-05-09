package stanford.infolab.debugger.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.io.UnsupportedEncodingException;

import javax.ws.rs.core.MediaType;

import sun.security.ssl.Debug;

import com.google.common.net.HttpHeaders;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/*
 * The Abstract class for HTTP handlers. 
 */
public abstract class ServerHttpHandler implements HttpHandler {
  // Response body.
  protected String response;
  // Response body as a byte array
  protected byte[] responseBytes;
  // Response status code. Please use HttpUrlConnection final static members.
  protected int statusCode;
  // MimeType of the response. Please use MediaType final static members.
  protected String responseMimeType;
  // HttpExchange object received in the handle call.
  protected HttpExchange httpExchange;

  /*
   * Handles an HTTP call's lifecycle - read parameters, process and send
   * response.
   */
  public void handle(HttpExchange httpExchange) throws IOException {
    // Assign class members so that subsequent methods can use it.
    this.httpExchange = httpExchange;
    this.setResponseMimeType();
    String rawUrl = httpExchange.getRequestURI().getQuery();
    HashMap<String, String> paramMap;
    int statusCode;

    try {
      paramMap = ServerUtils.getUrlParams(rawUrl);
      // Call the method implemented by inherited classes.
      processRequest(httpExchange, paramMap);
    } catch (UnsupportedEncodingException ex) {
      this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
      this.response = "Malformed URL. Given encoding is not supported.";
    }
    // In case of an error statusCode, we just write the exception string.
    // (Consider using JSON).
    if (this.statusCode != HttpURLConnection.HTTP_OK) {
      this.responseMimeType = MediaType.TEXT_PLAIN;
    }
    // Set mandatory Response Headers.
    this.setMandatoryResponseHeaders();
    // Write Text Response if responeMimeType is json or if the statusCode is
    // not OK.
    if (this.responseMimeType == MediaType.APPLICATION_JSON 
      || this.statusCode != HttpURLConnection.HTTP_OK) {
      this.writeTextResponse();
    } else {
      this.writeByteResponse();
    }
  }

  /*
   * Writes the text response.
   */
  private void writeTextResponse() throws IOException {
    this.httpExchange.sendResponseHeaders(this.statusCode, this.response.length());
    OutputStream os = this.httpExchange.getResponseBody();
    os.write(this.response.getBytes());
    os.close();
  }

  /*
   * Writes bytes response.
   */
  private void writeByteResponse() throws IOException {
    this.httpExchange.sendResponseHeaders(this.statusCode, this.responseBytes.length);
    OutputStream os = this.httpExchange.getResponseBody();
    os.write(this.responseBytes);
    os.close();
  }

  private void setResponseMimeType() {
    Headers requestHeaders = this.httpExchange.getRequestHeaders();
    // Set default mime type as application/json
    this.responseMimeType = MediaType.APPLICATION_JSON;
    // Set response mime type as octet-stream, if requested.
    if (requestHeaders.containsKey(HttpHeaders.ACCEPT)) {
      List<String> contentTypes = requestHeaders.get(HttpHeaders.ACCEPT);
      if (contentTypes.contains(MediaType.APPLICATION_OCTET_STREAM)) {
        this.responseMimeType = MediaType.APPLICATION_OCTET_STREAM;
      }
    }
  }

  /*
   * Add mandatory headers to the HTTP response by the debugger server. MUST be
   * called before sendResponseHeaders.
   */
  private void setMandatoryResponseHeaders() {
    // TODO(vikesh): **REMOVE CORS FOR ALL AFTER DECIDING THE DEPLOYMENT
    // ENVIRONMENT**
    Headers headers = this.httpExchange.getResponseHeaders();
    headers.add("Access-Control-Allow-Origin", "*");
    headers.add("Content-Type", this.responseMimeType);
  }

  /*
   * Implement this method in inherited classes. This method MUST set statusCode
   * and response class members appropriately.
   */
  public abstract void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap);
}
