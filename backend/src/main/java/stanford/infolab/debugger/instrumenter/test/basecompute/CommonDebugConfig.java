package stanford.infolab.debugger.instrumenter.test.basecompute;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import stanford.infolab.debugger.instrumenter.DebugConfig;

public class CommonDebugConfig
		extends
		DebugConfig<LongWritable, DoubleWritable, FloatWritable, DoubleWritable, DoubleWritable> {

	@Override
	public boolean shouldCatchExceptions() {
		return true;
	}

}
