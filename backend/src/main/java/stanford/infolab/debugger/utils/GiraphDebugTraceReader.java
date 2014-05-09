package stanford.infolab.debugger.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Temporary utility class to read the traces that the debugger has left.
 * 
 * @author semihsalihoglu
 */
public class GiraphDebugTraceReader {

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    String traceDir = "/giraph-debug-traces/job_201405072208_0009/";
    System.out.println("traceDir: " + traceDir);
    String coreSitePath = "/Users/semihsalihoglu/projects/hadoop-1.2.1/conf/core-site.xml";
    Configuration configuration = new Configuration();
    configuration.addResource(new Path(coreSitePath));
    FileSystem fs = FileSystem.get(configuration);
    Path pt = new Path(traceDir);
    for (FileStatus fileStatus: fs.listStatus(pt)) {
//      System.out.println("reading trace: " + fileStatus.getPath());
      GiraphScenarioWrapper scenarioWrapper = new GiraphScenarioWrapper();
      scenarioWrapper.load("/Users/semihsalihoglu/Downloads/tr_job0_stp_0_vid_0.tr");
//      scenarioWrapper.loadFromHDFS(fs, fileStatus.getPath().toString());
      System.out.println(scenarioWrapper);

//      MsgIntegrityViolationWrapper msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper();
//      msgIntegrityViolationWrapper.loadFromHDFS(fs, fileStatus.getPath().toString());
//      System.out.println(msgIntegrityViolationWrapper);

//      VertexValueIntegrityViolationWrapper vertexValueIntegrityViolationWrapper =
//        new VertexValueIntegrityViolationWrapper();
//      vertexValueIntegrityViolationWrapper.loadFromHDFS(fs, fileStatus.getPath().toString());
//      System.out.println(vertexValueIntegrityViolationWrapper);
    }
  }
}