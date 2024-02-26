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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class ObsoleteReachability<K> implements TGraphAlgorithm<K, Tuple2<Long, Long>, Integer, Long, DataSet<Vertex<K, Boolean>>> {

    private final K srcVertexId;

    private final int timeStep;
    private final static long travelTime = 1;

    public ObsoleteReachability(K srcVertexId, int timeStep) {
        this.srcVertexId = srcVertexId;
        this.timeStep = timeStep;
    }

    @Override
    public DataSet<Vertex<K, Boolean>> run(Tgraph<K, Tuple2<Long, Long>, Integer, Long> input) throws Exception {
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("Reachability");
        parameters.setSolutionSetUnmanagedMemory(true);

        return input.getGellyGraph().mapVertices(new InitVerticesMapper<>(srcVertexId, this.timeStep))
                .runScatterGatherIteration(new TravelViaEdge<>(this.timeStep),
                        new TravelViaVertex<>(this.timeStep), this.timeStep, parameters)
                .mapVertices(new FinalVerticesMapper<>()).getVertices();
    }

    private static final class InitVerticesMapper<K> implements MapFunction<Vertex<K, Tuple2<Long, Long>>,
            Tuple3<Long, Long, Set<Long>>> {
        private final K srcVertexId;
        private final int timeStep;

        public InitVerticesMapper(K srcId, int timeStep) {
            this.srcVertexId = srcId;
            this.timeStep = timeStep;
        }

        public Tuple3<Long, Long, Set<Long>> map(Vertex<K, Tuple2<Long, Long>> v) {
            Set<Long> arrivedAt = new HashSet<>();
            if (v.f0.equals(this.srcVertexId) && v.f1.f0 <= this.timeStep && this.timeStep < v.f1.f1)
                arrivedAt.add(v.f1.f0);
            return new Tuple3<>(v.f1.f0, v.f1.f1, arrivedAt);
        }
    }

    private static final class TravelViaEdge<K> extends ScatterFunction<K, Tuple3<Long, Long, Set<Long>>, List<Long>,
            Tuple3<Integer, Long, Long>> {

        private final int timeStep;

        private TravelViaEdge(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void sendMessages(Vertex<K, Tuple3<Long, Long, Set<Long>>> vertex) {
            long vStart = vertex.getValue().f0;
            long vEnd = vertex.getValue().f1;

            for (Edge<K, Tuple3<Integer, Long, Long>> edge : getEdges()) {
                long eStart = edge.getValue().f1;
                long eEnd = edge.getValue().f2;

                long intersectionStart = Math.max(vStart, eStart);
                long intersectionEnd = Math.min(vEnd, eEnd);

                List<Long> msgs = vertex.getValue().f2.stream()
                        .map(arrivalTime -> Math.max(arrivalTime, intersectionStart) + edge.getValue().f0)
                        .filter(arrivalTime -> arrivalTime < intersectionEnd && arrivalTime <= this.timeStep)
                        .collect(Collectors.toList());

                if (!msgs.isEmpty()) sendMessageTo(edge.getTarget(), msgs);
            }
        }
    }

    private static final class TravelViaVertex<K> extends GatherFunction<K, Tuple3<Long, Long, Set<Long>>, List<Long>> {

        private final int timeStep;

        public TravelViaVertex(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void updateVertex(Vertex<K, Tuple3<Long, Long, Set<Long>>> vertex, MessageIterator<List<Long>> inMessages) {
            Tuple3<Long, Long, Set<Long>> vertexProperties = vertex.getValue();
            for (List<Long> msg : inMessages) {
                List<Long> collect = msg.stream()
                        .filter(arrivalTime -> vertexProperties.f0 <= arrivalTime && arrivalTime < vertexProperties.f1)
                        .collect(Collectors.toList());

                if (!collect.isEmpty()) {
                    vertexProperties.f2.addAll(collect);
                    this.setNewVertexValue(vertexProperties);
                }
            }
        }
    }

    public static final class FinalVerticesMapper<K> implements MapFunction<Vertex<K, Tuple3<Long, Long, Set<Long>>>, Boolean> {
        @Override
        public Boolean map(Vertex<K, Tuple3<Long, Long, Set<Long>>> vertex) {
            return !vertex.getValue().f2.isEmpty();
        }
    }
}
