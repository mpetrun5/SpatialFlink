/*
Copyright 2020 Data Platform Research Team, AIRC, AIST, Japan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RangeQuery implements Serializable {

    //--------------- GRID-BASED RANGE QUERY -----------------//
    public static DataStream<Point> SpatialRangeQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint, guaranteedNeighboringCells);

        DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<Point> unaggregatedNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<Point, Point, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Point> neighbors) throws Exception {
                        for (Point point : pointIterator) {
                            if (guaranteedNeighboringCells.contains(point.gridID))
                                neighbors.collect(point);
                            else {
                                Double distance = HelperClass.computeEuclideanDistance(queryPoint.point.getX(), queryPoint.point.getY(), point.point.getX(),point.point.getY());
                                if (distance <= queryRadius)
                                { neighbors.collect(point);}
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");

        return unaggregatedNeighbours;
    }


    public static DataStream<String> GetCellsWithinRadius(DataStream<Point> pointStream, double queryRadius, long windowSize, long slideStep, UniformGrid uGrid) {

        DataStream<Point> spatialStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<String> windowedNeighboringCellIds = spatialStreamWithTsAndWm.keyBy(new KeySelector<Point, String>(){
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<Point, String, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<String> neighbors) throws Exception {

                        // Using set to avoid duplicate cell ids in the output
                        //HashSet<String> neighboringCells = new HashSet<String>();
                        for (Point point : pointIterator) {

                            HashSet<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, point);
                            HashSet<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, point, guaranteedNeighboringCells);

                            //System.out.println("guaranteedNeighboringCells " + guaranteedNeighboringCells);
                            //System.out.println("candidateNeighboringCells " + candidateNeighboringCells);

                            // Adding all the guaranteed cells into neighboringCells
                            //neighboringCells.addAll(guaranteedNeighboringCells);
                            for(String cellId: guaranteedNeighboringCells){
                                neighbors.collect(cellId);
                            }

                            for(String cellId: candidateNeighboringCells){
                                // Computing the distances between point(sensor location) and cell boundaries
                                List<Tuple2<Double, Double>> cellCoordinates = HelperClass.getCellCoordinates(cellId, uGrid);

                                // Iterating through the 4 coordinates of a cell, the 5th coordinate is equivalent to the first
                                for(int i = 0; i < 4; i++) {
                                    Tuple2<Double, Double> cellCoordinate = cellCoordinates.get(i);
                                    Double distance = HelperClass.computeEuclideanDistance(cellCoordinate.f0, cellCoordinate.f1, point.point.getX(), point.point.getY());
                                    //System.out.println("distance between cell and query object " + distance + " query radius " + queryRadius);
                                    if (distance <= queryRadius) {
                                        //neighboringCells.add(cellId);
                                        neighbors.collect(cellId);
                                        continue; // If a cell is added, go out of the for loop
                                    }
                                }
                            }
                        }
                        // Collecting the output
                        //for(String cellId: neighboringCells){
                        //    neighbors.collect(cellId);
                        //}
                    }
                }).name("Time Windowed Query Neighboring Cells");

        return windowedNeighboringCellIds;

        /*
        return windowedNeighboringCellIds.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep))).apply(new AllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<String> cellIds, Collector<String> neighboringCellsOutput) throws Exception {

                HashSet<String> neighboringCells = new HashSet<String>();
                for (String cellId : cellIds) {
                    neighboringCells.add(cellId);
                }

                // Collecting the output
                for(String cellId: neighboringCells){
                    neighboringCellsOutput.collect(cellId);
                }
            }
        });
        */
    }
}
