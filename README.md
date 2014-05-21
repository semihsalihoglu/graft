# Giraph Debugger

## Synopsis

### Build Backend
    cd backend
    mvn package

### Download a Sample Graph
    curl -L http://ece.northwestern.edu/~aching/shortestPathsInputGraph.tar.gz | tar xf -
    hadoop fs -put shortestPathsInputGraph shortestPathsInputGraph

### Launch Giraph's Shortest Path Example
    hadoop jar \
        target/backend-0.0-SNAPSHOT.jar org.apache.giraph.GiraphRunner \
        org.apache.giraph.examples.SimpleShortestPathsComputation \
        -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
        -vip shortestPathsInputGraph \
        -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
        -op shortestPathsOutputGraph.$RANDOM \
        -w 1 \
        -ca giraph.SplitMasterWorker=false \
        #

### Now, Launch It with Debugging
You can launch any Giraph program with debugging support by simply replacing the first two words (`hadoop jar`) of the command, specifying a class name for debugging configuration:

    ./giraph-debug stanford.infolab.debugger.examples.simpledebug.SimpleShortestPathsDebugConfig \
        target/backend-0.0-SNAPSHOT.jar org.apache.giraph.GiraphRunner \
        org.apache.giraph.examples.SimpleShortestPathsComputation \
        -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
        -vip shortestPathsInputGraph \
        -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
        -op shortestPathsOutputGraph.$RANDOM \
        -w 1 \
        -ca giraph.SplitMasterWorker=false \
        #

### Launch GUI
First launch the GUI with the following command:

    ./giraph-debug gui

Then open <http://localhost:8000> from your web browser.

You can use a different port by launching the GUI with:

    GUI_PORT=12345 ./giraph-debug gui

