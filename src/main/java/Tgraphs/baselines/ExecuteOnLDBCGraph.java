package Tgraphs.baselines;

import Tgraphs.baselines.algorithm.SingleSourceShortestPath;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Class for doing some benchmarking tests with the EAT algorithm
 * The class iterates over the different graphs indicated by @param graphs
 * the class then appends the results in the results.txt file for the results
 */

/*
 * ./flink/bin/flink run -c Tgraphs.baselines.ExecuteOnLDBCGraph /home/hadoop/jan-baseline/jars/tink/tink-ldbc-eat.jar 0 180
 */
@Deprecated
public class ExecuteOnLDBCGraph {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: ExecuteOnLDBCGraph <algorithmToRun> <start time step> <end time step>");
            System.exit(1);
        }
        int start = Integer.parseInt(args[0]);
        int end = Integer.parseInt(args[1]);

        /*
        Define some objects like input output files.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final String graphName = "LDBC";
        final String algorithmToRun = "SSSP";
        final String inputDirectory = "/data/hadoop/wicmi/ldbcMutationsProcessed/tink/time=";
        final String outputDirectory = "/home/hadoop/jan-baseline/results/tink/" + graphName + "/" + algorithmToRun;

        File resultsFile = new File(outputDirectory + "/tink_compute_time.csv");
        if (!resultsFile.getParentFile().exists()) {
            if (!resultsFile.getParentFile().mkdirs())
                throw new Exception("Could not create parent directories for output resultsFile");
        }

        FileWriter fr = new FileWriter(resultsFile, true);
        BufferedWriter br = new BufferedWriter(fr);

        long srcVertexId = 3165345L;
        for (int i = start; i <= end; i++) {
            BaselineExecutor
                    .loadTemporalGraph(inputDirectory + i, env)
                    .run(new SingleSourceShortestPath<>(srcVertexId, i))
                    .writeAsCsv(outputDirectory + "/results/" + i);

            br.write(i + "," + env.execute().getNetRuntime() + "\n");
            br.flush();
            if (i % 10 == 0) System.out.println("Finished " + i + " iterations.");
        }
        br.close();
        fr.close();
    }
}
