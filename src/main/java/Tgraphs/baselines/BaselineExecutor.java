package Tgraphs.baselines;

import Tgraphs.Tgraph;
import Tgraphs.baselines.algorithm.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class BaselineExecutor {

    public static Tgraph<Long, Tuple2<Long, Long>, Integer, Long> loadTemporalGraph(String inputDirectory,
                                                                                    ExecutionEnvironment env) throws Exception {
        DataSet<Tuple3<Long, Long, Long>> verticesInput = env.readCsvFile(inputDirectory + "/vertices.csv")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class, Long.class, Long.class); // read the node IDs as Longs

        DataSet<Vertex<Long, Tuple2<Long, Long>>> tinkVertices =
                verticesInput.map(new MapFunction<Tuple3<Long, Long, Long>, Vertex<Long, Tuple2<Long, Long>>>() {
                    @Override
                    public Vertex<Long, Tuple2<Long, Long>> map(Tuple3<Long, Long, Long> input) {
                        return new Vertex<>(input.f0, new Tuple2<>(input.f1, input.f2));
                    }
                });

        DataSet<Tuple4<Long, Long, Long, Long>> edgeInput = env.readCsvFile(inputDirectory + "/edges.csv")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class, Long.class, Long.class, Long.class); // read the node IDs as Longs

        DataSet<Edge<Long, Tuple3<Integer, Long, Long>>> tinkEdges =
                edgeInput.map(new MapFunction<Tuple4<Long, Long, Long, Long>, Edge<Long, Tuple3<Integer, Long, Long>>>() {
                    @Override
                    public Edge<Long, Tuple3<Integer, Long, Long>> map(Tuple4<Long, Long, Long, Long> input) {
                        return new Edge<>(input.f0, input.f1, new Tuple3<>(1, input.f2, input.f3));
                    }
                });

        return new Tgraph<>(tinkVertices, tinkEdges, env);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("Usage: ./flink/bin/flink run Tgraphs.baselines.BaselineExecutor <full-jar-path> <graph> " +
                    "<algorithm> <src vertex id> <start time step> <end time step> <increment>");
            System.out.println("Example: ./flink/bin/flink run -c Tgraphs.baselines.BaselineExecutor " +
                    "/home/hadoop/jan-baseline/jars/tink.jar Reddit Reachability 4912335 1 122 5");
            System.out.println("Example: ./flink/bin/flink run -c Tgraphs.baselines.BaselineExecutor " +
                    "/home/hadoop/jan-baseline/jars/tink.jar LDBC SSSP 3165345 1 365 10");
            System.exit(1);
        }
        final String graph = args[0];
        final String algorithm = args[1];
        final long srcVertexId = Long.parseLong(args[2]);
        final int start = Integer.parseInt(args[3]);
        final int end = Integer.parseInt(args[4]);
        final int increment = args.length == 6 ? Integer.parseInt(args[5]) : 1;

        final String inputDirectory = graph.equals("LDBC") ? "/data/hadoop/wicmi/ldbcMutationsProcessed/tink/time=" : "/data/hadoop/wicmi/tink/tinkRedditInputs/time=";
        final String outputDirectory = "/home/hadoop/jan-baseline/results/tink/" + graph + "/" + algorithm;

        final File resultsFile = new File(outputDirectory + "/compute_time_july.csv");
        if (!resultsFile.getParentFile().exists()) {
            if (!resultsFile.getParentFile().mkdirs())
                throw new Exception("Could not create parent directories for output resultsFile");
        }

        System.out.println("Graph ->" + graph);
        System.out.println("Algorithm ->" + algorithm);
        System.out.println("Source Vertex Id ->" + srcVertexId);
        System.out.println("Execution Range -> [" + start + ", " + end + "]");
        System.out.println("Input Dir. ->" + inputDirectory);
        System.out.println("Output Dir. ->" + outputDirectory);

        final FileWriter fr = new FileWriter(resultsFile, true);
        final BufferedWriter br = new BufferedWriter(fr);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        for (int i = start; i <= end; i+=increment) {
            switch (algorithm) {
                case "SSSP":
                    long s = System.currentTimeMillis();
                    Tgraph<Long, Tuple2<Long, Long>, Integer, Long> tGraph = BaselineExecutor.loadTemporalGraph(inputDirectory + i, env);

                    long e = System.currentTimeMillis();
                    System.out.println("Graph load time := " +  (e - s));

                    s = System.currentTimeMillis();
                    DataSet<Vertex<Long, Integer>> results = tGraph.run(new SingleSourceShortestPath<>(srcVertexId, i));
                    results.first(5);
                    e = System.currentTimeMillis();
                    System.out.println("Results time := " +  (e - s));

                    s = System.currentTimeMillis();
//                    results.writeAsCsv(outputDirectory + "/results-july/" + i);
                    e = System.currentTimeMillis();
                    System.out.println("Graph write time := " +  (e - s));
                    break;
                case "EAT":
                    BaselineExecutor
                            .loadTemporalGraph(inputDirectory + i, env)
                            .run(new EarliestArrivalTime<>(srcVertexId, i))
                            .writeAsCsv(outputDirectory + "/results-july/" + i);
                    break;
                case "TMST":
                    BaselineExecutor
                            .loadTemporalGraph(inputDirectory + i, env)
                            .run(new TemporalMST(srcVertexId, i))
                            .writeAsCsv(outputDirectory + "/results-july/" + i);
                    break;
                case "Reachability":
                    BaselineExecutor
                            .loadTemporalGraph(inputDirectory + i, env)
                            .run(new Reachability<>(srcVertexId, i))
                            .writeAsCsv(outputDirectory + "/results-july/" + i);
                    break;
                case "FAST":
                    BaselineExecutor
                            .loadTemporalGraph(inputDirectory + i, env)
                            .run(new FastestPathDuration<>(srcVertexId, i))
                            .writeAsCsv(outputDirectory + "/results-july/" + i);
                    break;
                default:
                    throw new IllegalArgumentException("Algorithm not supported");
            }
            String log = i + "," + env.execute().getNetRuntime() + "\n";
            br.write(log);
            System.out.println(log);
            br.flush();
            if (i % 10 == 0) System.out.println("Finished " + i + " iterations.");
        }
        br.close();
        fr.close();
    }
}
