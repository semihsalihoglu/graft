package stanford.infolab.debugger.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import stanford.infolab.debugger.instrumenter.AbstractInterceptingComputation;
import stanford.infolab.debugger.mock.ComputeTestGenerator;
import stanford.infolab.debugger.server.ServerUtils.DebugTrace;
import stanford.infolab.debugger.utils.GiraphVertexScenarioWrapper;

/**
 * This main class is the command line interface for the debugger. The command syntax is as follows:
 * list <job_id>
 * dump <job_id> <superstep> <vertex>
 * mktest <job_id> <superstep> <vertex> [output_prefix]
 * 
 * @author Brian Truong Ba Quan
 */
public class CommandLineMain {
  
  protected static final Logger LOG = Logger.getLogger(CommandLineMain.class);

  public static void main(String[] args) {
    // Validate
    if (args.length == 0 || (!args[0].equalsIgnoreCase("list") && !args[0].equalsIgnoreCase("dump") 
        && !args[0].equalsIgnoreCase("mktest")))
      printHelp();

    if (args.length <= 1)
      printHelp();

    String jobId = args[1];

    if (args[0].equalsIgnoreCase("list")) {
      try {
        FileSystem fs = ServerUtils.getFileSystem();
        String jobTracePath = ServerUtils.getTraceFileRoot(jobId, DebugTrace.REGULAR);
        Path traceFilePath = new Path(jobTracePath);
        FileStatus[] fileStatuses = fs.listStatus(traceFilePath);
        for (FileStatus status : fileStatuses) {
          LOG.info(status.getPath().getName());
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    } else {
      if (args.length <= 3)
        printHelp();

      Long superstepNo = Long.parseLong(args[2]);
      String vertexId = args[3];

      try {
        // Read scenario.
        // TODO: rename ServerUtils to Utils
        @SuppressWarnings("rawtypes")
        GiraphVertexScenarioWrapper scenarioWrapper =
            ServerUtils.readScenarioFromTrace(jobId, superstepNo, vertexId);
        if (scenarioWrapper == null) {
          LOG.error("The trace file does not exist.");
          System.exit(0);
        }

        if (args[0].equalsIgnoreCase("dump")) {
          LOG.info(scenarioWrapper);
        } else if (args[0].equalsIgnoreCase("mktest")) {
          // Read output prefix and test class.
          String outputPrefix = null;
          String testClass = null;
          if (args.length > 4) {
            outputPrefix = args[4].trim();
            testClass =
                outputPrefix.substring(outputPrefix.lastIndexOf('/') + 1, outputPrefix.length());
          }
          
          // Generate test case.
          ComputeTestGenerator generator = new ComputeTestGenerator();
          String generatedTestCase = generator.generateTest(scenarioWrapper, null, testClass);
          if (outputPrefix != null) {
            try (PrintWriter writer =
                new PrintWriter(new FileWriter(new File(outputPrefix + ".java")))) {
              writer.append(generatedTestCase);
            }
          }
          LOG.info(generatedTestCase);
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  private static void printHelp() {
    LOG.info("Supported commands: ");
    LOG.info("\tlist <job_id>");
    LOG.info("\t\tList available traces/scenarios (supersteps/vertices) for a job");
    LOG.info("\tdump <job_id> <superstep> <vertex>");
    LOG.info("\t\tDump a trace in textual form");
    LOG.info("\tmktest <job_id> <superstep> <vertex> [output_prefix]");
    LOG.info("\t\tGenerate a JUnit test case code from a trace. If an output_prefix is "
        + "provided, a .java file is generated at the specified path.");
    System.exit(0);
  }
}
