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
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class JoinQuery implements Serializable {

    // REAL-TIME
    //--------------- JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<Point, Point>> PointJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int omegaJoinDurationSeconds, UniformGrid uGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStream, queryRadius, uGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Point>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Point>>() {
            @Override
            public boolean filter(Tuple2<Point, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<Polygon, Point>> PolygonJoinQuery(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, int omegaJoinDurationSeconds, UniformGrid uGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> polygonStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Point>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Polygon, Point, Tuple2<Polygon, Point>>() {
                    @Override
                    public Tuple2<Polygon, Point> join(Polygon poly, Point q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPointPolygonBBoxMinEuclideanDistance(q, poly);
                        }else{
                            distance = DistanceFunctions.getDistance(q, poly);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Point>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<LineString, Point>> LineStringJoinQuery(DataStream<LineString> lineStringStream, DataStream<Point> queryPointStream, double queryRadius, int omegaJoinDurationSeconds, UniformGrid uGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> lineStringStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<LineString> replicatedLineStringStream = lineStringStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Point>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<LineString, Point, Tuple2<LineString, Point>>() {
                    @Override
                    public Tuple2<LineString, Point> join(LineString ls, Point q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPointLineStringBBoxMinEuclideanDistance(q, ls);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Point>>() {
            @Override
            public boolean filter(Tuple2<LineString, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<Point, Polygon>> PointJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, double queryRadius, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, uGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Polygon>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Polygon, Tuple2<Point,Polygon>>() {
                    @Override
                    public Tuple2<Point, Polygon> join(Point p, Polygon q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Point, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<Polygon, Polygon>> PolygonJoinQuery(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, double queryRadius, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryPolygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> ordinaryStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<Polygon> replicatedOrdinaryStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Polygon>> joinOutput = replicatedOrdinaryStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Polygon, Polygon, Tuple2<Polygon, Polygon>>() {
                    @Override
                    public Tuple2<Polygon, Polygon> join(Polygon poly, Polygon q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPolygonPolygonBBoxMinEuclideanDistance(q, poly);
                        }else{
                            distance = DistanceFunctions.getDistance(q, poly);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<LineString, Polygon>> LineStringJoinQuery(DataStream<LineString> lineStringStream, DataStream<Polygon> queryStream, UniformGrid uGrid, double queryRadius, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> ordinaryStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Polygon>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<LineString, Polygon, Tuple2<LineString, Polygon>>() {
                    @Override
                    public Tuple2<LineString, Polygon> join(LineString ls, Polygon q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPolygonLineStringBBoxMinEuclideanDistance(q, ls);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Polygon>>() {
            @Override
            public boolean filter(Tuple2<LineString, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<Point, LineString>> PointJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStream, queryRadius, uGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, LineString>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, LineString, Tuple2<Point,LineString>>() {
                    @Override
                    public Tuple2<Point, LineString> join(Point p, LineString q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, LineString>>() {
            @Override
            public boolean filter(Tuple2<Point, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<Polygon, LineString>> PolygonJoinQuery(DataStream<Polygon> polygonStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> ordinaryStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<Polygon> replicatedOrdinaryStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, LineString>> joinOutput = replicatedOrdinaryStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Polygon, LineString, Tuple2<Polygon, LineString>>() {
                    @Override
                    public Tuple2<Polygon, LineString> join(Polygon poly, LineString q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPolygonLineStringBBoxMinEuclideanDistance(poly, q);
                        }else{
                            distance = DistanceFunctions.getDistance(poly, q);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, LineString>>() {
            @Override
            public boolean filter(Tuple2<Polygon, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<LineString, LineString>> LineStringJoinQuery(DataStream<LineString> lineStringStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> ordinaryStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, LineString>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<LineString, LineString, Tuple2<LineString, LineString>>() {
                    @Override
                    public Tuple2<LineString, LineString> join(LineString ls, LineString q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getBBoxBBoxMinEuclideanDistance(q.boundingBox, ls.boundingBox);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, LineString>>() {
            @Override
            public boolean filter(Tuple2<LineString, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    // WINDOW BASED
    //--------------- JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<Point, Point>> PointJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStream, queryRadius, uGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Point>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Point>>() {
            @Override
            public boolean filter(Tuple2<Point, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<Polygon, Point>> PolygonJoinQuery(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> polygonStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Point>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Point, Tuple2<Polygon, Point>>() {
                    @Override
                    public Tuple2<Polygon, Point> join(Polygon poly, Point q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPointPolygonBBoxMinEuclideanDistance(q, poly);
                        }else{
                            distance = DistanceFunctions.getDistance(q, poly);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Point>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<LineString, Point>> LineStringJoinQuery(DataStream<LineString> lineStringStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> lineStringStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<LineString> replicatedLineStringStream = lineStringStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Point>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<LineString, Point, Tuple2<LineString, Point>>() {
                    @Override
                    public Tuple2<LineString, Point> join(LineString ls, Point q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPointLineStringBBoxMinEuclideanDistance(q, ls);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Point>>() {
            @Override
            public boolean filter(Tuple2<LineString, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<Point, Polygon>> PointJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, uGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Polygon>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Polygon, Tuple2<Point,Polygon>>() {
                    @Override
                    public Tuple2<Point, Polygon> join(Point p, Polygon q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Point, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<Polygon, Polygon>> PolygonJoinQuery(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryPolygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> ordinaryStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<Polygon> replicatedOrdinaryStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Polygon>> joinOutput = replicatedOrdinaryStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Polygon, Tuple2<Polygon, Polygon>>() {
                    @Override
                    public Tuple2<Polygon, Polygon> join(Polygon poly, Polygon q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPolygonPolygonBBoxMinEuclideanDistance(q, poly);
                        }else{
                            distance = DistanceFunctions.getDistance(q, poly);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<LineString, Polygon>> LineStringJoinQuery(DataStream<LineString> lineStringStream, DataStream<Polygon> queryStream, UniformGrid uGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> ordinaryStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Polygon>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<LineString, Polygon, Tuple2<LineString, Polygon>>() {
                    @Override
                    public Tuple2<LineString, Polygon> join(LineString ls, Polygon q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPolygonLineStringBBoxMinEuclideanDistance(q, ls);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Polygon>>() {
            @Override
            public boolean filter(Tuple2<LineString, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<Point, LineString>> PointJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStream, queryRadius, uGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, LineString>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, LineString, Tuple2<Point,LineString>>() {
                    @Override
                    public Tuple2<Point, LineString> join(Point p, LineString q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, LineString>>() {
            @Override
            public boolean filter(Tuple2<Point, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<Polygon, LineString>> PolygonJoinQuery(DataStream<Polygon> polygonStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> ordinaryStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<Polygon> replicatedOrdinaryStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, LineString>> joinOutput = replicatedOrdinaryStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, LineString, Tuple2<Polygon, LineString>>() {
                    @Override
                    public Tuple2<Polygon, LineString> join(Polygon poly, LineString q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getPolygonLineStringBBoxMinEuclideanDistance(poly, q);
                        }else{
                            distance = DistanceFunctions.getDistance(poly, q);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, LineString>>() {
            @Override
            public boolean filter(Tuple2<Polygon, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- JOIN QUERY - POLYGON STREAM - POINT QUERY STREAM -----------------//
    public static DataStream<Tuple2<LineString, LineString>> LineStringJoinQuery(DataStream<LineString> lineStringStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> ordinaryStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStreamWithTsAndWm, queryRadius, uGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, LineString>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<LineString, LineString, Tuple2<LineString, LineString>>() {
                    @Override
                    public Tuple2<LineString, LineString> join(LineString ls, LineString q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = HelperClass.getBBoxBBoxMinEuclideanDistance(q.boundingBox, ls.boundingBox);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, LineString>>() {
            @Override
            public boolean filter(Tuple2<LineString, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    /*
    //--------------- (MODIFIED) GRID-BASED JOIN QUERY - POINT-POLYGON -----------------//
    public static DataStream<Tuple2<String, String>> SpatialJoinQueryOptimized(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep){

        DataStream<Tuple2<Point,Boolean>> replicatedQueryStream = JoinQuery.getReplicatedQueryStreamModified(queryPointStream, queryRadius, uGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Tuple2<Point,Boolean>, String>() {
                    @Override
                    public String getKey(Tuple2<Point,Boolean> q) throws Exception {
                        return q.f0.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Tuple2<Point,Boolean>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Tuple2<Point,Boolean> q) {
                        if (q.f1 == true) {  // guaranteed neighbors
                            return Tuple2.of(poly.gridID, q.f0.gridID);
                        } else { // candidate neighbors
                            if (HelperClass.getPointPolygonBBoxMinEuclideanDistance(q.f0, poly) <= queryRadius) {
                                return Tuple2.of(poly.gridID, q.f0.gridID);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }*/


    /*
    //--------------- GRID-BASED JOIN QUERY - POLYGON-POLYGON -----------------//
    public static DataStream<Tuple2<String,String>> SpatialJoinQuery(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, int slideStep, int windowSize, double queryRadius, UniformGrid uGrid){
        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, uGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon query) throws Exception {
                        return query.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Polygon, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Polygon query) {
                        if (HelperClass.getBBoxBBoxMinEuclideanDistance(query.boundingBox, poly.boundingBox) <= queryRadius) {
                            return Tuple2.of(poly.gridID, query.gridID);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

     */

    /*
    //--------------- (MODIFIED) GRID-BASED JOIN QUERY - POLYGON-POLYGON -----------------//
    public static DataStream<Tuple2<String,String>> SpatialJoinQueryOptimized(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, int slideStep, int windowSize, double queryRadius, UniformGrid uGrid){
        DataStream<Tuple2<Polygon,Boolean>> replicatedQueryStream = JoinQuery.getReplicatedQueryStreamModified(queryPolygonStream, uGrid, queryRadius);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Tuple2<Polygon,Boolean>, String>() {
                    @Override
                    public String getKey(Tuple2<Polygon,Boolean> query) throws Exception {
                        return query.f0.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Tuple2<Polygon,Boolean>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Tuple2<Polygon,Boolean> query) {
                        if (query.f1 == true) {  // guaranteed neighbors
                            return Tuple2.of(poly.gridID, query.f0.gridID);
                        } else { // candidate neighbors
                            if (HelperClass.getBBoxBBoxMinEuclideanDistance(query.f0.boundingBox, poly.boundingBox) <= queryRadius) {
                                return Tuple2.of(poly.gridID, query.f0.gridID);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });

    }
     */


    //Replicate Query Point Stream for each Neighbouring Grid ID
    public static DataStream<Point> getReplicatedPointQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Point>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Point> out) throws Exception {

                // Neighboring cells contain all the cells including Candidate cells, Guaranteed Cells and the query point cell itself
                HashSet<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

                // Create duplicated query points
                for (String gridID: neighboringCells) {
                    //Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    Point p = new Point(queryPoint.objID, queryPoint.point.getX(), queryPoint.point.getY(), queryPoint.timeStampMillisec, gridID);
                    out.collect(p);
                }
            }
        });
    }


    //Replicate Query Polygon Stream for each Neighbouring Grid ID
    // TODO: delete unused code
    public static DataStream<Polygon> getReplicatedPolygonQueryStream(DataStream<Polygon> queryPolygons, double queryRadius, UniformGrid uGrid){

        return queryPolygons.flatMap(new RichFlatMapFunction<Polygon, Polygon>() {
            //private Integer parallelism;
            //private String uniqueObjID;

            /*
            @Override
            public void open(Configuration parameters) {
                RuntimeContext ctx = getRuntimeContext();
                parallelism = ctx.getNumberOfParallelSubtasks();
                uniqueObjID = Integer.toString(ctx.getIndexOfThisSubtask());
            }
             */

            @Override
            public void flatMap(Polygon poly, Collector<Polygon> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, poly);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, poly, guaranteedNeighboringCells);

                // Create duplicated polygon stream for all neighbouring cells based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    //Polygon p = new Polygon(poly.getCoordinates(), uniqueObjID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    Polygon p = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    out.collect(p);
                }
                for (String gridID: candidateNeighboringCells) {
                    //Polygon p = new Polygon(poly.getCoordinates(), uniqueObjID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    Polygon p = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    out.collect(p);
                }

                // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
                // uniqueObjID += parallelism;
            }
        });
    }


    public static DataStream<LineString> getReplicatedLineStringQueryStream(DataStream<LineString> queryLineString, double queryRadius, UniformGrid uGrid){

        return queryLineString.flatMap(new RichFlatMapFunction<LineString, LineString>() {

            @Override
            public void flatMap(LineString lineString, Collector<LineString> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, lineString);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, lineString, guaranteedNeighboringCells);

                // Create duplicated polygon stream for all neighbouring cells based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    LineString ls = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates().clone()), lineString.gridIDsSet, gridID, lineString.timeStampMillisec, lineString.boundingBox);
                    out.collect(ls);
                }
                for (String gridID: candidateNeighboringCells) {
                    LineString ls = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates().clone()), lineString.gridIDsSet, gridID, lineString.timeStampMillisec, lineString.boundingBox);
                    out.collect(ls);
                }
            }
        });
    }

    /*
    //Replicate Query Point Stream for each Candidate and Guaranteed Grid ID
    public static DataStream<Tuple2<Point,Boolean>> getReplicatedQueryStreamModified(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Tuple2<Point,Boolean>>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Tuple2<Point,Boolean>> out) throws Exception {

                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

                // Create duplicated query points for Guaranteed Neighbors
                for (String gridID: guaranteedNeighboringCells) {
                    Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    out.collect(Tuple2.of(p,true));
                }

                // Create duplicated query points for Candidate Neighbors
                for (String gridID: candidateNeighboringCells) {
                    Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    out.collect(Tuple2.of(p,false));
                }
            }
        });
    }
     */

    /*
    //Replicate Query Polygon Stream for each Candidate and Guaranteed Grid ID
    public static DataStream<Tuple2<Polygon,Boolean>> getReplicatedQueryStreamModified(DataStream<Polygon> queryPolygons, UniformGrid uGrid, double queryRadius){
        return queryPolygons.flatMap(new RichFlatMapFunction<Polygon, Tuple2<Polygon,Boolean>>() {
            private long parallelism;
            private int uniqueObjID;

            @Override
            public void open(Configuration parameters) {
                RuntimeContext ctx = getRuntimeContext();
                parallelism = ctx.getNumberOfParallelSubtasks();
                uniqueObjID = Integer.toString(ctx.getIndexOfThisSubtask());
            }

            @Override
            public void flatMap(Polygon poly, Collector<Tuple2<Polygon,Boolean>> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, poly);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, poly, guaranteedNeighboringCells);

                // Create duplicated polygon stream based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    Polygon p = new Polygon(poly.getCoordinates(), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                    out.collect(Tuple2.of(p,true));
                }
                for (String gridID: candidateNeighboringCells) {
                    Polygon p = new Polygon(poly.getCoordinates(), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                    out.collect(Tuple2.of(p,false));
                }

                // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
                uniqueObjID += parallelism;
            }
        });
    }
     */

}


