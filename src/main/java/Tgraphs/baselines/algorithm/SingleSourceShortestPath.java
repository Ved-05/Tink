package Tgraphs.baselines.algorithm;

import Tgraphs.TGraphAlgorithm;
import Tgraphs.Tgraph;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

import java.util.*;
import java.util.stream.Collectors;


public class SingleSourceShortestPath<K> implements TGraphAlgorithm<K, Tuple2<Long, Long>, Integer, Long,
        DataSet<Vertex<K, Integer>>> {

    private final K srcVertexId;

    private final int timeSteps;

    /**
     * Creates an instance of the SingleSourceShortestTemporalPathEAT algorithm
     *
     * @param srcVertexId The ID of the source vertex.
     */
    public SingleSourceShortestPath(K srcVertexId, int timeSteps) {
        this.srcVertexId = srcVertexId;
        this.timeSteps = timeSteps;
    }

    @Override
    public DataSet<Vertex<K, Integer>> run(Tgraph<K, Tuple2<Long, Long>, Integer, Long> input) throws Exception {
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("Single Source Shortest Path");
        parameters.setSolutionSetUnmanagedMemory(true);

        long s = System.currentTimeMillis();
        Graph<K, Tuple2<Long, Long>, Tuple3<Integer, Long, Long>> gellyGraph = input.getGellyGraph();
        gellyGraph.getVertices().first(5);
        long e = System.currentTimeMillis();
        System.out.println("Gelly Graph conversion := " + (e - s));

        return gellyGraph.mapVertices(new InitVerticesMapper<>(srcVertexId))
                .runScatterGatherIteration(
                        new MinDistanceMessengerforTuplewithpath<>(),
                        new VertexDistanceUpdaterwithpath<>(this.timeSteps), this.timeSteps, parameters)
                .mapVertices(new FinalVerticesMapper<>())
                .getVertices();
    }

    private static final class InitVerticesMapper<K> implements MapFunction<Vertex<K, Tuple2<Long, Long>>,
            Tuple3<Long, Long, Map<Long, Integer>>> {
        private final K srcVertexId;

        public InitVerticesMapper(K srcId) {
            this.srcVertexId = srcId;
        }

        public Tuple3<Long, Long, Map<Long, Integer>> map(Vertex<K, Tuple2<Long, Long>> v) {
            Map<Long, Integer> arrivedAt = new HashMap<>();
            if (v.f0.equals(this.srcVertexId))
                arrivedAt.put(v.f1.f0, 0);

            return new Tuple3<>(v.f1.f0, v.f1.f1, arrivedAt);
        }
    }

    private static final class MinDistanceMessengerforTuplewithpath<K> extends ScatterFunction<K,
            Tuple3<Long, Long, Map<Long, Integer>>, Map<Long, Integer>, Tuple3<Integer, Long, Long>> {
        @Override
        public void sendMessages(Vertex<K, Tuple3<Long, Long, Map<Long, Integer>>> vertex) {
            Map<Long, Integer> cArrivedAtToDistance = vertex.getValue().f2;
            for (Edge<K, Tuple3<Integer, Long, Long>> edge : getEdges()) {
                long eStartTime = edge.getValue().f1;
                Map<Long, Integer> payload = cArrivedAtToDistance.entrySet().stream()
                        .filter(entry -> entry.getKey() < edge.getValue().f2)
                        .map(entry -> new HashMap.SimpleEntry<>(Math.max(entry.getKey(), eStartTime) + edge.getValue().f0,
                                entry.getValue() + edge.getValue().f0))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::min));
                if (!payload.isEmpty()) this.sendMessageTo(edge.getTarget(), payload);
            }
        }
    }

    private static final class VertexDistanceUpdaterwithpath<K> extends GatherFunction<K, Tuple3<Long, Long, Map<Long, Integer>>, Map<Long, Integer>> {

        private final int timeStep;

        public VertexDistanceUpdaterwithpath(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void updateVertex(Vertex<K, Tuple3<Long, Long, Map<Long, Integer>>> vertex, MessageIterator<Map<Long, Integer>> inMessages) {
            Tuple3<Long, Long, Map<Long, Integer>> vertexProperties = vertex.f1;
            Map<Long, Integer> cArrivedAtToDistance = vertexProperties.f2;

            boolean isPropertiesUpdated = false;
            for (Map<Long, Integer> msg : inMessages) {
                for (Map.Entry<Long, Integer> incomingEntry : msg.entrySet()) {
                    long incomingTime = incomingEntry.getKey();
                    int incomingDistance = incomingEntry.getValue();
                    if (incomingTime <= this.timeStep &&
                            vertexProperties.f0 <= incomingTime && incomingTime < vertexProperties.f1) {
                        if (cArrivedAtToDistance.containsKey(incomingTime)) {
                            if (incomingDistance < cArrivedAtToDistance.get(incomingTime)) {
                                vertexProperties.f2.put(incomingTime, incomingDistance);
                                isPropertiesUpdated = true;
                            }
                        } else {
                            // find the closest key that is less than the incoming time
                            Long closestKey = cArrivedAtToDistance.keySet().stream()
                                    .filter(key -> key < incomingTime)
                                    .max(Long::compareTo).orElse(null);
                            if (closestKey == null || incomingDistance < cArrivedAtToDistance.get(closestKey)) {
                                Long closestGreaterKey = cArrivedAtToDistance.keySet().stream()
                                        .filter(key -> key > incomingTime)
                                        .min(Long::compareTo).orElse(null);
                                if (closestGreaterKey == null || incomingDistance > cArrivedAtToDistance.get(closestGreaterKey)) {
                                    vertexProperties.f2.put(incomingTime, incomingDistance);
                                    isPropertiesUpdated = true;
                                } else {
                                    vertexProperties.f2.remove(closestGreaterKey);
                                    vertexProperties.f2.put(incomingTime, incomingDistance);
                                    isPropertiesUpdated = true;
                                }


                            }
                        }
                    }
                }
            }

            if (isPropertiesUpdated) this.setNewVertexValue(vertexProperties);
        }

    }


    private static final class FinalVerticesMapper<K> implements MapFunction<Vertex<K, Tuple3<Long, Long, Map<Long, Integer>>>, Integer> {

        @Override
        public Integer map(Vertex<K, Tuple3<Long, Long, Map<Long, Integer>>> v) {
            // get the maximum key in the map
            Map<Long, Integer> f2 = v.getValue().f2;
            Long maxKey = f2.keySet().stream().max(Long::compareTo).orElse(null);
            return maxKey == null ? Integer.MAX_VALUE : f2.get(maxKey);
        }
    }
}
