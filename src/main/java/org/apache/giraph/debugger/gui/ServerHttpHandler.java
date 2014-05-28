package org.apache.giraph.debugger.gui;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.util.HashMap;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import sun.security.ssl.Debug;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/*
 * The Abstract class for HTTP handlers. 
 */
public abstract class ServerHttpHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(ServerHttpHandler.class);
  // Response body.
  protected String response;
  // Response body as a byte array
  protected byte[] responseBytes;
  // Response status code. Please use HttpUrlConnection final static members.
  protected int statusCode;
  // MimeType of the response. Please use MediaType final static members.
  protected String responseContentType;
  // HttpExchange object received in the handle call.
  protected HttpExchange httpExchange;

  /*
   * Handles an HTTP call's lifecycle - read parameters, process and send
   * response.
   */
  public void handle(HttpExchange httpExchange) throws IOException {
    // Assign class members so that subsequent methods can use it.
    this.httpExchange = httpExchange;
    // Set application/json as the default content type.
    this.responseContentType = MediaType.APPLICATION_JSON;
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
      this.responseContentType = MediaType.TEXT_PLAIN;
    }
    // Set mandatory Response Headers.
    this.setMandatoryResponseHeaders();
    this.writeResponse();
  }

  /*
   * Writes the text response.
   */
  private void writeResponse() throws IOException {
    OutputStream os = this.httpExchange.getResponseBody();
    if (this.responseContentType == MediaType.APPLICATION_JSON 
      || this.responseContentType == MediaType.TEXT_PLAIN) {
      this.httpExchange.sendResponseHeaders(this.statusCode, this.response.length());
      os.write(this.response.getBytes());
    } else if(this.responseContentType == MediaType.APPLICATION_OCTET_STREAM) {
      this.httpExchange.sendResponseHeaders(this.statusCode, this.responseBytes.length);
      os.write(this.responseBytes);
    }
    os.close();
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
    headers.add("Content-Type", this.responseContentType);
  }
  
  /*
   * Sets the given headerKey to the given headerValue.
   * @param {String} headerKey - Header Key
   * @param {String} headerValue - Header Value.
   * @desc - For example, call like this to set the Content-disposition header
   * setResponseHeader("Content-disposition", "attachment");
   */
  protected void setResponseHeader(String headerKey, String headerValue) {
    Headers responseHeaders = this.httpExchange.getResponseHeaders();
    responseHeaders.add(headerKey, headerValue);
  }
  
  /*
   * Handle the common exceptions in processRequest. 
   * @params {String} [illegalArgumentMessage] - Message when illegal argument 
   * exception is thrown. Optional - May be null. 
   */
  protected void handleException(Exception e, String illegalArgumentMessage) {
    LOG.error(e);
    if (e instanceof IllegalArgumentException) {
      this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
      this.response = illegalArgumentMessage;
    } else if(e instanceof NumberFormatException) { 
      this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
      this.response = String.format("%s must be an integer >= -1.", ServerUtils.SUPERSTEP_ID_KEY);
    } else if(e instanceof FileNotFoundException) {
      this.statusCode = HttpURLConnection.HTTP_NOT_FOUND;
      this.response = "File not found on the server. Please ensure this vertex/master was debugged.";
    } else if (e instanceof IOException || e instanceof InstantiationException 
      || e instanceof IllegalAccessException || e instanceof ClassNotFoundException) {
        this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        this.response = "Internal Server Error.";
    } else {
      Debug.println("Unknown Exception", e.toString());
      this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      this.response = "Unknown exception occured.";
    }
  }
  
  /*
   * Implement this method in inherited classes. This method MUST set statusCode
   * and response (or responseBytes) class members appropriately. In case the Content type
   * is not JSON, must specify the new Content type. Default type is application/json.
   * Non-200 Status is automatically assigned text/plain. 
   */
  public abstract void processRequest(HttpExchange httpExchange, HashMap<String, String> paramMap);
}
