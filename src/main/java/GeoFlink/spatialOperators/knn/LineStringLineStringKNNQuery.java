package GeoFlink.spatialOperators.knn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.Comparators;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LineStringLineStringKNNQuery extends KNNQuery<LineString, LineString> {
    public LineStringLineStringKNNQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeKNNQuery(conf, index);
    }

    public DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> run(DataStream<LineString> lineStringStream, LineString queryLineString, double queryRadius, Integer k) throws IOException {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(lineStringStream, queryLineString, queryRadius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - LINESTRING - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(lineStringStream, queryLineString, queryRadius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> realTime(DataStream<LineString> lineStringStream, LineString queryLineString, double queryRadius, Integer k, UniformGrid uGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
        Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

        DataStream<LineString> streamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new HelperClass.cellBasedLineStringFlatMap(neighboringCells));

        DataStream<PriorityQueue<Tuple2<LineString, Double>>> windowedKNN = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new WindowFunction<LineString, PriorityQueue<Tuple2<LineString, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<LineString, Double>> kNNPQ = new PriorityQueue<Tuple2<LineString, Double>>(k, new Comparators.inTupleLineStringDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> inputTuples, Collector<PriorityQueue<Tuple2<LineString, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (LineString lineString : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, lineString.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(queryLineString, lineString);
                                }

                                kNNPQ.offer(new Tuple2<LineString, Double>(lineString, distance));
                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, lineString.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(queryLineString, lineString);
                                }
                                //double largestDistInPQ = HelperClass.getPointPolygonMinEuclideanDistance(queryPoint, kNNPQ.peek().f0);
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                assert kNNPQ.peek() != null;
                                double largestDistInPQ = kNNPQ.peek().f1;

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<LineString, Double>(lineString, distance));
                                }
                            }
                        }



                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -

        //Output kNN Stream
        return windowedKNN
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new kNNWinAllEvaluationLineStringStream(k));
    }

    // WINDOW BASED
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> windowBased(DataStream<LineString> lineStringStream, LineString queryLineString, double queryRadius, Integer k, UniformGrid uGrid, int windowSize, int windowSlideStep, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
        Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

        DataStream<LineString> streamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new HelperClass.cellBasedLineStringFlatMap(neighboringCells));

        DataStream<PriorityQueue<Tuple2<LineString, Double>>> windowedKNN = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<LineString, PriorityQueue<Tuple2<LineString, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<LineString, Double>> kNNPQ = new PriorityQueue<Tuple2<LineString, Double>>(k, new Comparators.inTupleLineStringDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> inputTuples, Collector<PriorityQueue<Tuple2<LineString, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (LineString lineString : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, lineString.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(queryLineString, lineString);
                                }

                                kNNPQ.offer(new Tuple2<LineString, Double>(lineString, distance));
                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, lineString.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(queryLineString, lineString);
                                }
                                //double largestDistInPQ = HelperClass.getPointPolygonMinEuclideanDistance(queryPoint, kNNPQ.peek().f0);
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                assert kNNPQ.peek() != null;
                                double largestDistInPQ = kNNPQ.peek().f1;

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<LineString, Double>(lineString, distance));
                                }
                            }
                        }



                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -

        //Output kNN Stream
        return windowedKNN
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new kNNWinAllEvaluationLineStringStream(k));
    }
}
