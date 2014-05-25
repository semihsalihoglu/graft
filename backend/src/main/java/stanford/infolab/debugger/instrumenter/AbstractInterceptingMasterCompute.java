package stanford.infolab.debugger.instrumenter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import stanford.infolab.debugger.utils.ExceptionWrapper;
import stanford.infolab.debugger.utils.GiraphMasterScenarioWrapper;

/**
 * Class that intercepts calls to {@link MasterCompute}'s exposed methods for
 * GiraphDebugger.
 * 
 * @author semihsalihoglu
 */
public abstract class AbstractInterceptingMasterCompute extends MasterCompute {

  protected static final Logger LOG = Logger.getLogger(AbstractInterceptingMasterCompute.class);
  private GiraphMasterScenarioWrapper giraphMasterScenarioWrapper;
  private CommonVertexMasterInterceptionUtil commonVertexMasterInterceptionUtil;
  
  /**
   * Called immediately as user's {@link MasterCompute#compute()} method is entered.
   */
  public void interceptComputeBegin() {
    System.out.println("AbstractInterceptingMasterCompute.interceptInitializeEnd() called");
    LOG.info(this.getClass().getName() + ".interceptInitializeEnd is called ");
    giraphMasterScenarioWrapper = new GiraphMasterScenarioWrapper(this.getClass().getName());
    if (commonVertexMasterInterceptionUtil == null) {
      commonVertexMasterInterceptionUtil = new CommonVertexMasterInterceptionUtil(getContext()
        .getJobID().toString());
    }
    System.out.println("superstepNo: " + getSuperstep());
    commonVertexMasterInterceptionUtil.initCommonVertexMasterContextWrapper(getConf(),
      getSuperstep(), getTotalNumVertices(), getTotalNumEdges());
    giraphMasterScenarioWrapper.setCommonVertexMasterContextWrapper(
      commonVertexMasterInterceptionUtil.getCommonVertexMasterContextWrapper());
  }

  @Intercept(renameTo="getAggregatedValue")
  //@Override
  public <A extends Writable> A getAggregatedValueIntercept(String name) {
    A retVal = super.<A> getAggregatedValue(name);
    commonVertexMasterInterceptionUtil.addAggregatedValueIfNotExists(name, retVal);
    return retVal;
  }

  /**
   * Called when user's {@link MasterCompute#compute()} method throws an exception.
   *
   * @param e exception thrown.
   */
  final protected void interceptComputeException(Exception e) {
    LOG.info("LOG.info: Caught an exception in user's MasterCompute. message: " + e.getMessage()
      + ". Saving a trace in HDFS.");
    ExceptionWrapper exceptionWrapper = new ExceptionWrapper(e.getMessage(),
      ExceptionUtils.getStackTrace(e));
    giraphMasterScenarioWrapper.setExceptionWrapper(exceptionWrapper);
    commonVertexMasterInterceptionUtil.saveMasterScenarioWrapper(giraphMasterScenarioWrapper,
      true /* contains an exception */);
  }

  /**
   * Called after user's {@link MasterCompute#compute()} method returns.
   */
  public void interceptComputeEnd() {
    commonVertexMasterInterceptionUtil.saveMasterScenarioWrapper(giraphMasterScenarioWrapper,
      false /* does not contain an exception */);
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {}

  @Override
  public void write(DataOutput arg0) throws IOException {}
}