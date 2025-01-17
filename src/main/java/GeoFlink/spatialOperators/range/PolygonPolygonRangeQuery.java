package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PolygonPolygonRangeQuery extends RangeQuery<Polygon, Polygon> {
    public PolygonPolygonRangeQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<Polygon> run(DataStream<Polygon> polygonStream, Polygon queryPolygon, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();
        //--------------- Real-time - POLYGON - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells)).startNewChain();

            return filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).flatMap(new FlatMapFunction<Polygon, Polygon>() {
                @Override
                public void flatMap(Polygon poly, Collector<Polygon> collector) throws Exception {

                    int cellIDCounter = 0;
                    for(String polyGridID: poly.gridIDsSet) {

                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                            cellIDCounter++;
                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if(cellIDCounter == poly.gridIDsSet.size()){
                                collector.collect(poly);
                            }
                        }
                        else { // candidate neighbors
                            double distance;
                            if(approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, poly.boundingBox);
                            }else{
                                distance = DistanceFunctions.getDistance(poly, queryPolygon);
                            }

                            if (distance <= queryRadius){
                                collector.collect(poly);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - POINT - POLYGON");
        }
        //--------------- WINDOW-based - POLYGON - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

            return filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> polygonIterator, Collector<Polygon> neighbors) throws Exception {
                            for (Polygon poly : polygonIterator) {
                                int cellIDCounter = 0;
                                for(String polyGridID: poly.gridIDsSet) {

                                    if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                        cellIDCounter++;
                                        // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if(cellIDCounter == poly.gridIDsSet.size()){
                                            neighbors.collect(poly);
                                        }
                                    }
                                    else { // candidate neighbors
                                        double distance;
                                        if(approximateQuery) {
                                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, poly.boundingBox);
                                        }else{
                                            distance = DistanceFunctions.getDistance(poly, queryPolygon);
                                        }

                                        if (distance <= queryRadius){
                                            neighbors.collect(poly);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POLYGON");
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }
}
