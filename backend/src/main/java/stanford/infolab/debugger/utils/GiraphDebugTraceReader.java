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

  public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    String traceDir = "/giraph-debug-traces/job_201405120024_0010/";
    System.out.println("traceDir: " + traceDir);
    String coreSitePath = "/Users/semihsalihoglu/projects/hadoop-1.2.1/conf/core-site.xml";
    Configuration configuration = new Configuration();
    configuration.addResource(new Path(coreSitePath));
    FileSystem fs = FileSystem.get(configuration);
    Path pt = new Path(traceDir);
    for (FileStatus fileStatus: fs.listStatus(pt)) {
//      System.out.println("reading trace: " + fileStatus.getPath());
      GiraphVertexScenarioWrapper scenarioWrapper = new GiraphVertexScenarioWrapper();
//      scenarioWrapper.load("/Users/semihsalihoglu/Downloads/download");
      scenarioWrapper.loadFromHDFS(fs, fileStatus.getPath().toString());
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