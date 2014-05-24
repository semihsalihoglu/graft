package stanford.infolab.debugger.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import stanford.infolab.debugger.mock.TestCaseGenerator;
import stanford.infolab.debugger.server.ServerUtils.DebugTrace;
import stanford.infolab.debugger.utils.GiraphVertexScenarioWrapper;

public class CommandLineMain {

  public static void main(String[] args) {
    // Validate
    if (args.length == 0
        || (!args[0].equalsIgnoreCase("list") && !args[0].equalsIgnoreCase("dump") && !args[0]
            .equalsIgnoreCase("mktest")))
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
          System.out.println(status.getPath().getName());
        }
      } catch (IOException e) {
        e.printStackTrace(System.err);
      }
    } else {
      if (args.length <= 3)
        printHelp();

      Long superstepNo = Long.parseLong(args[2]);
      String vertexId = args[3];

      try {
        // Read scenario.
        @SuppressWarnings("rawtypes")
        GiraphVertexScenarioWrapper scenarioWrapper =
            ServerUtils.readScenarioFromTrace(jobId, superstepNo, vertexId);
        if (scenarioWrapper == null) {
          System.err.println("The trace file does not exist.");
          System.exit(0);
        }

        if (args[0].equalsIgnoreCase("dump")) {
          System.out.println(scenarioWrapper);
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
          TestCaseGenerator generator = new TestCaseGenerator();
          String generatedTestCase = generator.generateTest(scenarioWrapper, null, testClass);
          if (outputPrefix != null) {
            try (PrintWriter writer =
                new PrintWriter(new FileWriter(new File(outputPrefix + ".java")))) {
              writer.append(generatedTestCase);
            }
          }
          System.out.println(generatedTestCase);
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IOException e) {
        System.err.println(e.getMessage());
      }
    }
  }

  private static void printHelp() {
    System.out.println("Supported commands: ");
    System.out.println("\tLIST <JOB_ID>");
    System.out.println("\t\tList available traces/scenarios (supersteps/vertices) for a job");
    System.out.println("\tDUMP <JOB_ID> <SUPERSTEP> <VERTEX>");
    System.out.println("\t\tDump a trace in textual form");
    System.out.println("\tMKTEST <JOB_ID> <SUPERSTEP> <VERTEX> [OUTPUT_PREFIX]");
    System.out.println("\t\tGenerate a JUnit test case code from a trace. If an OUTPUT_PREFIX is "
        + "provided, a .java file is generated at the specified path.");
    System.exit(0);
  }
}
