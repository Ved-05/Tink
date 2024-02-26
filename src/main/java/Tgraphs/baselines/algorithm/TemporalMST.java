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

public class TemporalMST implements TGraphAlgorithm<Long, Tuple2<Long, Long>, Integer, Long,
        DataSet<Vertex<Long, Tuple2<Long, Long>>>> {

    private final Long srcVertexId;

    private final int maxIterations;

    /**
     * Creates an instance of the SingleSourceShortestTemporalPathEAT algorithm
     *
     * @param srcVertexId The ID of the source vertex.
     */
    public TemporalMST(Long srcVertexId, int maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<Long, Tuple2<Long, Long>>> run(Tgraph<Long, Tuple2<Long, Long>, Integer, Long> input) throws Exception {
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("Temporal MST");
        parameters.setSolutionSetUnmanagedMemory(true);

        return input.getGellyGraph().mapVertices(new InitVerticesMapper(srcVertexId))
                .runScatterGatherIteration(new MinDistanceMessengerForTupleWithSrcVertex(),
                        new VertexDistanceUpdaterWithSrcVertex(this.maxIterations), maxIterations, parameters)
                .mapVertices(new FinalVerticesMapper()).getVertices();
    }

    private static final class InitVerticesMapper implements MapFunction<Vertex<Long, Tuple2<Long, Long>>,
            Tuple3<Long, Long, Tuple2<Long, Long>>> {
        private final Long srcVertexId;

        public InitVerticesMapper(Long srcId) {
            this.srcVertexId = srcId;
        }

        public Tuple3<Long, Long, Tuple2<Long, Long>> map(Vertex<Long, Tuple2<Long, Long>> v) {
            return new Tuple3<>(v.f1.f0, v.f1.f1,
                    v.f0.equals(this.srcVertexId) ? new Tuple2<>(srcVertexId, 0L) : new Tuple2<>(-1L, Long.MAX_VALUE));
        }
    }

    private static final class MinDistanceMessengerForTupleWithSrcVertex extends ScatterFunction<Long, Tuple3<Long, Long, Tuple2<Long, Long>>,
            Tuple2<Long, Long>, Tuple3<Integer, Long, Long>> {
        @Override
        public void sendMessages(Vertex<Long, Tuple3<Long, Long, Tuple2<Long, Long>>> vertex) {
            Tuple2<Long, Long> cArrivedAt = vertex.f1.f2;
            if (cArrivedAt.f1 < Integer.MAX_VALUE) {
                for (Edge<Long, Tuple3<Integer, Long, Long>> edge : getEdges()) {
                    long eStartTime = edge.f2.f1;
                    long targetArrivalTime = Math.max(cArrivedAt.f1, eStartTime) + edge.f2.f0;
                    if (cArrivedAt.f1 < eStartTime || cArrivedAt.f1 < edge.f2.f2)
                        this.sendMessageTo(edge.getTarget(), new Tuple2<>(vertex.f0, targetArrivalTime));
                }
            }
        }
    }

    private static final class VertexDistanceUpdaterWithSrcVertex extends GatherFunction<Long, Tuple3<Long, Long, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

        private final int timeStep;

        public VertexDistanceUpdaterWithSrcVertex(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void updateVertex(Vertex<Long, Tuple3<Long, Long, Tuple2<Long, Long>>> vertex, MessageIterator<Tuple2<Long, Long>> inMessages) {
            Tuple3<Long, Long, Tuple2<Long, Long>> vertexProperties = vertex.f1;
            Tuple2<Long, Long> currentPair = vertexProperties.f2;

            for (Tuple2<Long, Long> msg : inMessages) {
                long msgTime = msg.f1;
                long currentTime = currentPair.f1;

                if (msgTime <= this.timeStep &&
                        vertexProperties.f0 <= msgTime && msgTime < vertexProperties.f1) {
                    if (msgTime < currentTime)
                        currentPair = msg;
                    else if (msgTime == currentTime) {
                        if (msg.f0 < currentPair.f0)
                            currentPair = msg;
                    }
                }
            }
            if (currentPair != vertexProperties.f2) {
                vertexProperties.f2 = currentPair;
                this.setNewVertexValue(vertexProperties);
            }
        }
    }

    public static final class FinalVerticesMapper implements MapFunction<Vertex<Long, Tuple3<Long, Long, Tuple2<Long, Long>>>, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> map(Vertex<Long, Tuple3<Long, Long, Tuple2<Long, Long>>> v) {
            return v.getValue().f2;
        }
    }
}
