package Tgraphs.baselines.algorithm;

import Tgraphs.TGraphAlgorithm;
import Tgraphs.Tgraph;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

public class EarliestArrivalTime<K> implements TGraphAlgorithm<K, Tuple2<Long, Long>, Integer, Long,
        DataSet<Vertex<K, Double>>> {

    private final K srcVertexId;

    private final int maxIterations;

    /**
     * Creates an instance of the SingleSourceShortestTemporalPathEAT algorithm
     *
     * @param srcVertexId The ID of the source vertex.
     */
    public EarliestArrivalTime(K srcVertexId, int maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Double>> run(Tgraph<K, Tuple2<Long, Long>, Integer, Long> input) throws Exception {
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("EAT");
        parameters.setSolutionSetUnmanagedMemory(true);

        return input.getGellyGraph().mapVertices(new InitVerticesMapper<>(srcVertexId))
                .runScatterGatherIteration(new MinDistanceMessengerforTuplewithpath<>(),
                        new VertexDistanceUpdaterwithpath<>(this.maxIterations), maxIterations, parameters)
                .mapVertices(new FinalVerticesMapper<>()).getVertices();
    }

    private static final class InitVerticesMapper<K> implements MapFunction<Vertex<K, Tuple2<Long, Long>>,
            Tuple3<Long, Long, Long>> {
        private final K srcVertexId;

        public InitVerticesMapper(K srcId) {
            this.srcVertexId = srcId;
        }

        public Tuple3<Long, Long, Long> map(Vertex<K, Tuple2<Long, Long>> v) {
            return new Tuple3<>(v.f1.f0, v.f1.f1, v.f0.equals(this.srcVertexId) ? 0L : Long.MAX_VALUE);
        }
    }

    private static final class MinDistanceMessengerforTuplewithpath<K> extends ScatterFunction<K, Tuple3<Long, Long, Long>,
            Long, Tuple3<Integer, Long, Long>> {
        @Override
        public void sendMessages(Vertex<K, Tuple3<Long, Long, Long>> vertex) {
            Long cArrivedAt = vertex.f1.f2;
            if (cArrivedAt < Long.MAX_VALUE) {
                for (Edge<K, Tuple3<Integer, Long, Long>> edge : getEdges()) {
                    long eStartTime = edge.f2.f1;
                    long targetArrivalTime = Math.max(cArrivedAt, eStartTime) + edge.f2.f0;
                    if (cArrivedAt < eStartTime || cArrivedAt < edge.f2.f2)
                        this.sendMessageTo(edge.getTarget(), targetArrivalTime);
                }
            }
        }
    }

    /**
     * Gatherfunction of the SSSTPEAT
     * checks all incomming messages and stores
     * <p>
     * K as K
     * VV as Tuple2<Double,ArrayList<K>>
     * Message as Tuple2<Double,ArrayList<K>>
     *
     * @param <K>
     */
    private static final class VertexDistanceUpdaterwithpath<K> extends GatherFunction<K, Tuple3<Long, Long, Long>, Long> {

        private final int timeStep;

        public VertexDistanceUpdaterwithpath(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void updateVertex(Vertex<K, Tuple3<Long, Long, Long>> vertex, MessageIterator<Long> inMessages) {
            long minIncomingTime = Long.MAX_VALUE;

            for (Long msg : inMessages) {
                if (msg < minIncomingTime) minIncomingTime = msg;
            }

            Tuple3<Long, Long, Long> vertexProperties = vertex.f1;
            long currentTime = vertexProperties.f2;
            if (minIncomingTime < currentTime &&
                    minIncomingTime <= this.timeStep &&
                    vertexProperties.f0 <= minIncomingTime && minIncomingTime < vertexProperties.f1) {
                vertexProperties.f2 = minIncomingTime;
                this.setNewVertexValue(vertexProperties);
            }
        }
    }

    public static final class FinalVerticesMapper<K> implements MapFunction<Vertex<K, Tuple3<Long, Long, Long>>, Double> {
        @Override
        public Double map(Vertex<K, Tuple3<Long, Long, Long>> v) {
            return Double.valueOf(v.f1.f2);
        }
    }
}
