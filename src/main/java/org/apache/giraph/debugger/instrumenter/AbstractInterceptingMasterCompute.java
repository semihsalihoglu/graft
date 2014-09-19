/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.debugger.instrumenter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.ExceptionWrapper;
import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Class that intercepts calls to {@link MasterCompute}'s exposed methods for
 * GiraphDebugger.
 * 
 * @author semihsalihoglu
 */
public abstract class AbstractInterceptingMasterCompute extends MasterCompute {

  protected static final Logger LOG = Logger
    .getLogger(AbstractInterceptingMasterCompute.class);
  private GiraphMasterScenarioWrapper giraphMasterScenarioWrapper;
  private CommonVertexMasterInterceptionUtil commonVertexMasterInterceptionUtil;

  /**
   * Called immediately as user's {@link MasterCompute#compute()} method is
   * entered.
   */
  public void interceptComputeBegin() {
    LOG.info(this.getClass().getName() + ".interceptInitializeEnd is called ");
    giraphMasterScenarioWrapper = new GiraphMasterScenarioWrapper(this
      .getClass().getName());
    if (commonVertexMasterInterceptionUtil == null) {
      commonVertexMasterInterceptionUtil = new CommonVertexMasterInterceptionUtil(
        getContext().getJobID().toString());
    }
    commonVertexMasterInterceptionUtil.initCommonVertexMasterContextWrapper(
      getConf(), getSuperstep(), getTotalNumVertices(), getTotalNumEdges());
    giraphMasterScenarioWrapper
      .setCommonVertexMasterContextWrapper(commonVertexMasterInterceptionUtil
        .getCommonVertexMasterContextWrapper());
  }

  @Intercept(renameTo = "getAggregatedValue")
  // @Override
  public <A extends Writable> A getAggregatedValueIntercept(String name) {
    A retVal = super.<A> getAggregatedValue(name);
    commonVertexMasterInterceptionUtil.addAggregatedValueIfNotExists(name,
      retVal);
    return retVal;
  }

  /**
   * Called when user's {@link MasterCompute#compute()} method throws an
   * exception.
   * 
   * @param e
   *          exception thrown.
   */
  final protected void interceptComputeException(Exception e) {
    LOG.info("Caught an exception in user's MasterCompute. message: " +
      e.getMessage() + ". Saving a trace in HDFS.");
    ExceptionWrapper exceptionWrapper = new ExceptionWrapper(e.getMessage(),
      ExceptionUtils.getStackTrace(e));
    giraphMasterScenarioWrapper.setExceptionWrapper(exceptionWrapper);
    commonVertexMasterInterceptionUtil.saveScenarioWrapper(
      giraphMasterScenarioWrapper, DebuggerUtils.getFullMasterTraceFileName(
        DebugTrace.MASTER_EXCEPTION,
        commonVertexMasterInterceptionUtil.getJobId(), getSuperstep()));
  }

  /**
   * Called after user's {@link MasterCompute#compute()} method returns.
   */
  public void interceptComputeEnd() {
    commonVertexMasterInterceptionUtil.saveScenarioWrapper(
      giraphMasterScenarioWrapper, DebuggerUtils.getFullMasterTraceFileName(
        DebugTrace.MASTER_REGULAR,
        commonVertexMasterInterceptionUtil.getJobId(), getSuperstep()));
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
  }
}
