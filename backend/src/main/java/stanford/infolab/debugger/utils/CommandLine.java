package stanford.infolab.debugger.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import stanford.infolab.debugger.mock.ComputationComputeTestGenerator;
import stanford.infolab.debugger.server.ServerUtils;
import stanford.infolab.debugger.server.ServerUtils.DebugTrace;

/**
 * This main class is the command line interface for the debugger. The command syntax is as follows:
 * list <job_id>
 * dump <job_id> <superstep> <vertex>
 * mktest <job_id> <superstep> <vertex> [output_prefix]
 * 
 * @author Brian Truong Ba Quan
 * @author netj
 */
public class CommandLine {
  
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
      ArrayList<Long> superstepsDebugged = ServerUtils.getSuperstepsDebugged(jobId);
      for (Long superstepNo : superstepsDebugged) {
        System.out.println(String.format("%-15s  %s  %4d  TestMasterSuperstep%d", "mktest-master", jobId, superstepNo, superstepNo));
      }
      for (Long superstepNo : superstepsDebugged) {
      ArrayList<String> vertexIds = ServerUtils.getVerticesDebugged(
          jobId, superstepNo, DebugTrace.REGULAR);
      for (String vertexId : vertexIds) {
        System.out.println(String.format("%-15s  %s  %4d %8s", "dump", jobId, superstepNo, vertexId));
      }
      }
    for (Long superstepNo : superstepsDebugged) {
      ArrayList<String> vertexIds = ServerUtils.getVerticesDebugged(
          jobId, superstepNo, DebugTrace.REGULAR);
      for (String vertexId : vertexIds) {
        System.out.println(String.format("%-15s  %s  %4d %8s  Test_%s_S%d_V%s", "mktest", jobId, superstepNo, vertexId, jobId, superstepNo, vertexId));
      }
    }
      } catch (IOException e) {
        e.printStackTrace();
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
          ComputationComputeTestGenerator generator = new ComputationComputeTestGenerator();
          String generatedTestCase = generator.generateTest(scenarioWrapper, null, testClass);
          if (outputPrefix != null) {
            String filename = outputPrefix + ".java";
			try (PrintWriter writer =
                new PrintWriter(new FileWriter(new File(filename)))) {
              writer.append(generatedTestCase);
            }
            System.err.println("Wrote " + filename);
          } else {
            System.out.println(generatedTestCase);
          }
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IOException e) {
          e.printStackTrace();
      }
    }
  }

  private static void printHelp() {
    System.out.println("Supported commands: ");
    System.out.println("\tlist <job_id>");
    System.out.println("\t\tList available traces/scenarios (supersteps/vertices) for a job");
    System.out.println("\tdump <job_id> <superstep> <vertex>");
    System.out.println("\t\tDump a trace in textual form");
    System.out.println("\tmktest <job_id> <superstep> <vertex> [output_prefix]");
    System.out.println("\t\tGenerate a JUnit test case code from a trace. If an output_prefix is "
        + "provided, a .java file is generated at the specified path.");
    System.exit(0);
  }
}
