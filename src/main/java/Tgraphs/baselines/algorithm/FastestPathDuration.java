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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FastestPathDuration<K> implements TGraphAlgorithm<K, Tuple2<Long, Long>, Integer, Long,
        DataSet<Vertex<K, Integer>>> {

    private final K srcVertexId;

    private final int timeSteps;

    /**
     * Creates an instance of the SingleSourceShortestTemporalPathEAT algorithm
     *
     * @param srcVertexId The ID of the source vertex.
     */
    public FastestPathDuration(K srcVertexId, int timeSteps) {
        this.srcVertexId = srcVertexId;
        this.timeSteps = timeSteps;
    }

    @Override
    public DataSet<Vertex<K, Integer>> run(Tgraph<K, Tuple2<Long, Long>, Integer, Long> input) throws Exception {
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("Fastest Path Duration");
        parameters.setSolutionSetUnmanagedMemory(true);

        return input
                .getGellyGraph()
                .mapVertices(new InitVerticesMapper<>(srcVertexId))
                .runScatterGatherIteration(
                        new MinDistanceMessengerForTupleWithPath<>(),
                        new VertexDistanceUpdaterwithpath<>(this.timeSteps), this.timeSteps, parameters)
                .mapVertices(new FinalVerticesMapper<>())
                .getVertices();
    }

    private static final class InitVerticesMapper<K> implements MapFunction<Vertex<K, Tuple2<Long, Long>>,
            Tuple3<Long, Long, Map<Long, Long>>> {
        private final K srcVertexId;

        public InitVerticesMapper(K srcId) {
            this.srcVertexId = srcId;
        }

        public Tuple3<Long, Long, Map<Long, Long>> map(Vertex<K, Tuple2<Long, Long>> v) {
            Map<Long, Long> arrivedAt = new HashMap<>();
            if (v.f0.equals(this.srcVertexId)) {
                long vertexStart = v.getValue().f0;
                long vertexEnd = v.getValue().f1;
                while (vertexStart < vertexEnd) {
                    arrivedAt.put(vertexStart, vertexStart);
                    vertexStart++;
                }
            }
            return new Tuple3<>(v.f1.f0, v.f1.f1, arrivedAt);
        }
    }

    private static final class MinDistanceMessengerForTupleWithPath<K> extends ScatterFunction<K,
            Tuple3<Long, Long, Map<Long, Long>>, Map<Long, Long>, Tuple3<Integer, Long, Long>> {
        @Override
        public void sendMessages(Vertex<K, Tuple3<Long, Long, Map<Long, Long>>> vertex) {
            Map<Long, Long> cArrivedAtToDistance = vertex.getValue().f2;

            for (Edge<K, Tuple3<Integer, Long, Long>> edge : getEdges()) {
                long eStartTime = edge.getValue().f1;
                long eTravelTime = edge.getValue().f0;
                Map<Long, Long> payload = cArrivedAtToDistance.entrySet().stream()
                        .filter(entry -> entry.getValue() < edge.getValue().f2)
                        .map(entry -> new HashMap.SimpleEntry<>(entry.getKey(),
                                Math.max(entry.getValue(), eStartTime) + eTravelTime))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::min));
                if (!payload.isEmpty()) this.sendMessageTo(edge.getTarget(), payload);
            }
        }

    }

    private static final class VertexDistanceUpdaterwithpath<K> extends GatherFunction<K, Tuple3<Long, Long, Map<Long, Long>>, Map<Long, Long>> {

        private final int timeStep;

        public VertexDistanceUpdaterwithpath(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void updateVertex(Vertex<K, Tuple3<Long, Long, Map<Long, Long>>> vertex, MessageIterator<Map<Long, Long>> inMessages) {
            Tuple3<Long, Long, Map<Long, Long>> vertexProperties = vertex.f1;
            Map<Long, Long> cArrivedAtToDistance = vertexProperties.f2;

            boolean isPropertiesUpdated = false;
            for (Map<Long, Long> msg : inMessages) {
                for (Map.Entry<Long, Long> incomingEntry : msg.entrySet()) {
                    long incomingSourceStart = incomingEntry.getKey();
                    long incomingTime = incomingEntry.getValue();
                    if (incomingTime <= this.timeStep &&
                            vertexProperties.f0 <= incomingTime && incomingTime < vertexProperties.f1) {
                        if (cArrivedAtToDistance.containsKey(incomingSourceStart)) {
                            if (incomingTime < cArrivedAtToDistance.get(incomingSourceStart)) {
                                vertexProperties.f2.put(incomingSourceStart, incomingTime);
                                isPropertiesUpdated = true;
                            }
                        } else {
//                            // find the closest key that is less than the incoming time
//                            Long closestKey = cArrivedAtToDistance.keySet().stream()
//                                    .filter(key -> key < incomingTime)
//                                    .max(Long::compareTo).orElse(null);
//                            if (closestKey == null || incomingDistance < cArrivedAtToDistance.get(closestKey)) {
//                                Long closestGreaterKey = cArrivedAtToDistance.keySet().stream()
//                                        .filter(key -> key > incomingTime)
//                                        .min(Long::compareTo).orElse(null);
//                                if (closestGreaterKey != null && incomingDistance <= cArrivedAtToDistance.get(closestGreaterKey)) {
//                                    vertexProperties.f2.remove(closestGreaterKey);
//                                }
                            vertexProperties.f2.put(incomingSourceStart, incomingTime);
                            isPropertiesUpdated = true;
                        }
                    }
                }
            }

            if (isPropertiesUpdated) this.setNewVertexValue(vertexProperties);
        }

    }


    private static final class FinalVerticesMapper<K> implements MapFunction<Vertex<K, Tuple3<Long, Long, Map<Long, Long>>>, Integer> {
        @Override
        public Integer map(Vertex<K, Tuple3<Long, Long, Map<Long, Long>>> v) {
            return v.getValue().f2.entrySet().stream().map(entry -> entry.getValue() - entry.getKey())
                    .min(Long::compareTo).orElse(Long.MAX_VALUE).intValue();
        }
    }
}
