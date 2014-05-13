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
