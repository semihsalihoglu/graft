package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.debugger.utils.AggregatedValueWrapper;
import org.apache.giraph.debugger.utils.BaseWrapper;
import org.apache.giraph.debugger.utils.CommonVertexMasterContextWrapper;
import org.apache.giraph.debugger.utils.DebuggerUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Common class used by both {@link AbstractInterceptingComputation} and
 * {@link AbstractInterceptingMasterCompute}. Serves following functions: 
 * <ul>
 *   <li> Maintains a {@link CommonVertexMasterContextWrapper} which contains common information
 *   captured by both the Master and the Vertex class, such as aggregators that the user accesses,
 *   superstepNo, totalNumberOfVertices and edges. 
 *   <li> Contains helper methods to save a master or vertex trace file to HDFS and maintains a
 *   {@link FileSystem} object that can be used to write other traces to HDFS.
 *   <li> Contains a helper method to return the trace directory for a particular job.
 * </ul>
 *
 * TODO: We might consider adding a method to {@link AbstractComputation} and {@link MasterCompute}
 * to return all registered aggregators, such as getAllRegisteredAggregators. Right now we do
 * not intercept aggregators that were never called.
 * @author semihsalihoglu
 */
public class CommonVertexMasterInterceptionUtil {
  private static final Logger LOG = Logger.getLogger(AbstractInterceptingMasterCompute.class);
  private static FileSystem fileSystem = null;
  private String jobId;
  private ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers;
  private CommonVertexMasterContextWrapper commonVertexMasterContextWrapper;

  // Warning: Caller's should create a new object at least once each superstep.
  public CommonVertexMasterInterceptionUtil(String jobId) {
    this.jobId = jobId;
    previousAggregatedValueWrappers = new ArrayList<>();
    if (fileSystem == null) {
      try {
        fileSystem = FileSystem.get(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void initCommonVertexMasterContextWrapper(
    ImmutableClassesGiraphConfiguration immutableClassesConfig, long superstepNo,
    long totalNumVertices, long totalNumEdges) {
    this.commonVertexMasterContextWrapper =
      new CommonVertexMasterContextWrapper(immutableClassesConfig, superstepNo, totalNumVertices,
        totalNumEdges);
    commonVertexMasterContextWrapper.setPreviousAggregatedValues(previousAggregatedValueWrappers);
  }
 
  public <A extends Writable> void addAggregatedValueIfNotExists(String name, A retVal) {
    if (getPreviousAggregatedValueWrapper(name) == null && retVal != null) {
      previousAggregatedValueWrappers.add(new AggregatedValueWrapper(name, retVal));
    }
  }

  private AggregatedValueWrapper getPreviousAggregatedValueWrapper(String key) {
    for (AggregatedValueWrapper previousAggregatedValueWrapper : previousAggregatedValueWrappers) {
      if (key.equals(previousAggregatedValueWrapper.getKey())) {
        return previousAggregatedValueWrapper;
      }
    }
    return null;
  }

  public void saveScenarioWrapper(BaseWrapper masterOrVertexScenarioWrapper, String fullFileName) {
    LOG.info("saving trace at: " + fullFileName);
    try {
      masterOrVertexScenarioWrapper.saveToHDFS(fileSystem, fullFileName);
    } catch (IOException e) {
      LOG.error("Could not save the " + masterOrVertexScenarioWrapper.getClass().getName()
        + " protobuf trace. IOException was thrown. exceptionMessage: " + e.getMessage());
      e.printStackTrace();
    }
  }
  
  public ArrayList<AggregatedValueWrapper> getPreviousAggregatedValueWrappers() {
    return previousAggregatedValueWrappers;
  }

  public void setPreviousAggregatedValueWrappers(ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers) {
    this.previousAggregatedValueWrappers = previousAggregatedValueWrappers;
  }

  public CommonVertexMasterContextWrapper getCommonVertexMasterContextWrapper() {
    return commonVertexMasterContextWrapper;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public String getJobId() {
    return jobId;
  }
}
