package org.apache.giraph.debugger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.giraph.debugger.gui.ServerUtils;
import org.apache.giraph.debugger.mock.ComputationComputeTestGenerator;
import org.apache.giraph.debugger.mock.MasterComputeTestGenerator;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This main class is the command line interface for the debugger. The command
 * syntax is as follows: list <job_id> dump <job_id> <superstep> <vertex> mktest
 * <job_id> <superstep> <vertex> [output_prefix]
 * 
 * @author Brian Truong Ba Quan
 * @author netj
 */
public class CommandLine {

  public static void main(String[] args) {
    // Validate
    String mode = args[0];
    if (args.length == 0
      || (!mode.equalsIgnoreCase("list") && !mode.equalsIgnoreCase("dump")
        && !mode.equalsIgnoreCase("mktest") && !mode.equalsIgnoreCase("dump-master") && !mode
          .equalsIgnoreCase("mktest-master")))
      printHelp();

    if (args.length <= 1)
      printHelp();

    String jobId = args[1];

    if (mode.equalsIgnoreCase("list")) {
      try {
        List<Long> superstepsDebuggedMaster = ServerUtils.getSuperstepsMasterDebugged(jobId);
        Set<Long> superstepsDebugged = Sets.newHashSet(ServerUtils.getSuperstepsDebugged(jobId));
        superstepsDebugged.addAll(superstepsDebuggedMaster);
        List<Long> allSupersteps = Lists.newArrayList(superstepsDebugged);
        Collections.sort(allSupersteps);
        for (Long superstepNo : allSupersteps) {
          if (superstepsDebuggedMaster.contains(superstepNo)) {
            System.out.println(String.format("%-15s  %s  %4d           ", "dump-master", jobId,
              superstepNo));
            System.out.println(String.format("%-15s  %s  %4d           TestMaster_%s_S%d",
              "mktest-master", jobId, superstepNo, jobId, superstepNo));
          }
          List<DebugTrace> debugTraces = Arrays.asList( //
            DebugTrace.INTEGRITY_MESSAGE_SINGLE_VERTEX //
            , DebugTrace.INTEGRITY_VERTEX //
            , DebugTrace.VERTEX_EXCEPTION //
            , DebugTrace.VERTEX_REGULAR //
            );
          for (DebugTrace debugTrace : debugTraces) {
            for (String vertexId : ServerUtils.getVerticesDebugged(jobId, superstepNo, debugTrace)) {
              System.out.println(String.format("%-15s  %s  %4d %8s  # %s", "dump", jobId,
                superstepNo, vertexId, debugTrace));
              System.out.println(String.format("%-15s  %s  %4d %8s  Test_%s_S%d_V%s", "mktest",
                jobId, superstepNo, vertexId, jobId, superstepNo, vertexId));
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      if (args.length <= 2)
        printHelp();

      Long superstepNo = Long.parseLong(args[2]);
      try {
        if (mode.equalsIgnoreCase("dump") || mode.equalsIgnoreCase("mktest")) {
          if (args.length <= 3)
            printHelp();
          String vertexId = args[3];
          // Read scenario.
          // TODO: rename ServerUtils to Utils
          @SuppressWarnings("rawtypes")
          GiraphVertexScenarioWrapper scenarioWrapper = ServerUtils.readScenarioFromTrace(jobId,
            superstepNo, vertexId, DebugTrace.VERTEX_ALL);
          if (scenarioWrapper == null) {
            System.err.println("The trace file does not exist.");
            System.exit(2);
          }

          if (mode.equalsIgnoreCase("dump")) {
            System.out.println(scenarioWrapper);
          } else if (mode.equalsIgnoreCase("mktest")) {
            // Read output prefix and test class.
            if (args.length <= 4)
              printHelp();
            String outputPrefix = args[4].trim();
            String testClassName = new File(outputPrefix).getName();
            // Generate test case.
            String generatedTestCase = new ComputationComputeTestGenerator().generateTest(
              scenarioWrapper, null, testClassName);
            outputTestCase(outputPrefix, generatedTestCase);
          }
        } else if (mode.equalsIgnoreCase("dump-master") || mode.equalsIgnoreCase("mktest-master")) {
          GiraphMasterScenarioWrapper scenarioWrapper = ServerUtils.readMasterScenarioFromTrace(
            jobId, superstepNo, DebugTrace.MASTER_ALL);
          if (scenarioWrapper == null) {
            System.err.println("The trace file does not exist.");
            System.exit(2);
          }

          if (mode.equalsIgnoreCase("dump-master")) {
            System.out.println(scenarioWrapper);
          } else if (mode.equalsIgnoreCase("mktest-master")) {
            if (args.length <= 3)
              printHelp();
            String outputPrefix = args[3].trim();
            String testClassName = new File(outputPrefix).getName();
            String generatedTestCase = new MasterComputeTestGenerator().generateTest(
              scenarioWrapper, null, testClassName);
            outputTestCase(outputPrefix, generatedTestCase);
          }
        } else {
          printHelp();
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | IOException e) {
        e.printStackTrace();
      }
    }
  }

  protected static void outputTestCase(String outputPrefix, String generatedTestCase)
    throws IOException {
    if (outputPrefix != null) {
      String filename = outputPrefix + ".java";
      try (PrintWriter writer = new PrintWriter(new FileWriter(new File(filename)))) {
        writer.append(generatedTestCase);
      }
      System.err.println("Wrote " + filename);
    } else {
      System.out.println(generatedTestCase);
    }
  }

  private static void printHelp() {
    System.out.println("Supported commands: ");
    System.out.println("\tlist <job_id>");
    System.out.println("\t\tList available traces/scenarios (supersteps/vertices) for a job");
    System.out.println("\tdump <job_id> <superstep> <vertex>");
    System.out.println("\t\tDump a trace in textual form");
    System.out.println("\tmktest <job_id> <superstep> <vertex> <output_prefix>");
    System.out.println("\t\tGenerate a JUnit test case code from a trace. If an output_prefix is "
      + "provided, a .java file is generated at the specified path.");
    System.exit(1);
  }
}
