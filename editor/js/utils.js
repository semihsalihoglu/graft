/*
 * Utility functions
 */
function Utils() {}

/*
 * Counts the number of keys of a JSON object.
 */
Utils.count = function(obj) {
   var count=0;
   for(var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
         ++count;
      }
   }
   return count;
}

/*
 * Format feature for JS strings. 
 * Example - "Hello {0}, {1}".format("World", "Graph")
 * = Hello World, Graph
 */
if (!String.prototype.format) {
  String.prototype.format = function() {
    var args = arguments;
    return this.replace(/{(\d+)}/g, function(match, number) { 
      return typeof args[number] != 'undefined'
        ? args[number]
        : match
      ;
    });
  };
}

/*! jQuery Ajax Retry - v0.2.4 - 2013-08-16
* https://github.com/johnkpaul/jquery-ajax-retry
* Copyright (c) 2013 John Paul; Licensed MIT */
(function($) {
  // enhance all ajax requests with our retry API
  $.ajaxPrefilter(function(options, originalOptions, jqXHR){
    jqXHR.retry = function(opts){
      if(opts.timeout){
        this.timeout = opts.timeout;
      }
      if (opts.statusCodes) {
        this.statusCodes = opts.statusCodes;
      }
      if (opts.retryCallback) {
        this.retryCallback = opts.retryCallback;
      }
      return this.pipe(null, pipeFailRetry(this, opts));
    };
  });

  // generates a fail pipe function that will retry `jqXHR` `times` more times
  function pipeFailRetry(jqXHR, opts){
    var times = opts.times;
    var timeout = opts.timeout;
    var retryCallback = opts.retryCallback;

    // takes failure data as input, returns a new deferred
    return function(input, status, msg){
      var ajaxOptions = this;
      var output = new $.Deferred();

      // whenever we do make this request, pipe its output to our deferred
      function nextRequest() {
        $.ajax(ajaxOptions)
          .retry({times : times-1, timeout : timeout, retryCallback : retryCallback})
          .pipe(output.resolve, output.reject);
      }

      if (times > 1 && (!jqXHR.statusCodes || $.inArray(input.status, jqXHR.statusCodes) > -1)) {
        if (retryCallback) {
          retryCallback(times - 1);
        }
        // time to make that next request...
        if(jqXHR.timeout !== undefined){
          setTimeout(nextRequest, jqXHR.timeout);
        } else {
          nextRequest();
        }
      } else {
        // no times left, reject our deferred with the current arguments
        output.rejectWith(this, arguments);
      }
      return output;
    };
  }
}(jQuery));
