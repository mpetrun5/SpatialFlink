package GeoFlink.spatialOperators.knn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.Comparators;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Set;

public class PointPointKNNQuery extends KNNQuery<Point, Point> {
    public PointPointKNNQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeKNNQuery(conf, index);
    }

    public DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> run(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k) throws IOException {
       boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POINT - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(pointStream, queryPoint, queryRadius, k, uGrid, omegaJoinDurationSeconds, allowedLateness);
        }

        //--------------- Window-based - POINT - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, queryPoint, queryRadius, k, uGrid, windowSize, windowSlideStep, allowedLateness);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> realTime(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, UniformGrid uGrid, int omegaJoinDurationSeconds, int allowedLateness) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedKNN = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point p : inputTuples) {

                            if (kNNPQ.size() < k) {
                                double distance = DistanceFunctions.getDistance(queryPoint, p);
                                kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                            } else {
                                double distance = DistanceFunctions.getDistance(queryPoint, p);
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                assert kNNPQ.peek() != null;
                                double largestDistInPQ = kNNPQ.peek().f1;

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
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
                .apply(new kNNWinAllEvaluationPointStream(k));
    }

    // WINDOW BASED
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> windowBased(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, UniformGrid uGrid, int windowSize, int windowSlideStep, int allowedLateness) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        //filteredPoints.print();
        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedKNN = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point p : inputTuples) {

                            if (kNNPQ.size() < k) {
                                double distance = DistanceFunctions.getDistance(queryPoint, p);
                                kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                                //System.out.println(p + ", dist: " + distance);
                            } else {
                                double distance = DistanceFunctions.getDistance(queryPoint, p);
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek

                                assert kNNPQ.peek() != null;
                                double largestDistInPQ = kNNPQ.peek().f1;
                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");

        //windowedKNN.print();
        // windowAll to Generate integrated kNN -

        //Output kNN Stream
        return windowedKNN
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new kNNWinAllEvaluationPointStream(k));
    }
}

