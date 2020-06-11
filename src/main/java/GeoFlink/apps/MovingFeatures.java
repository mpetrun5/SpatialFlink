package GeoFlink.apps;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MovingFeatures implements Serializable {

    //--------------- MOVING FEATURES STAY TIME -----------------//
    public static DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> CellBasedStayTime(DataStream<Point> pointStream, String aggregateFunction, String windowType, long windowSize, long windowSlideStep) {

        //TODO
        // aggregateFunction
        // windowType


        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> spatialStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        if(windowType.equalsIgnoreCase("COUNT")){

            DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> cWindowedCellBasedStayTime = spatialStreamWithTsAndWm
                    .keyBy(new gridCellKeySelector())
                    .countWindow(windowSize, windowSlideStep)
                    .process(new CountWindowProcessFunction(aggregateFunction)).name("Count Windowed Moving Features");

            return cWindowedCellBasedStayTime;

        }
        else { // Default TIME Window

            DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> tWindowedCellBasedStayTime = spatialStreamWithTsAndWm
                    .keyBy(new gridCellKeySelector())
                    .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                    .process(new TimeWindowProcessFunction(aggregateFunction)).name("Time Windowed Moving Features");

            return tWindowedCellBasedStayTime;
        }
    }


    // Key selector
    public static class gridCellKeySelector implements KeySelector<Point,String> {
        @Override
        public String getKey(Point p) throws Exception {
            return p.gridID;
        }
    }

    // Count Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    public static class CountWindowProcessFunction extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, String, GlobalWindow> {

        // HashMap<TrackerID, timestamp>
        HashMap<Integer, Long> minTimestampTrackerID = new HashMap<Integer, Long>();
        HashMap<Integer, Long> maxTimestampTrackerID = new HashMap<Integer, Long>();
        // HashMap <TrackerID, TrajLength>
        HashMap<Integer, Long> trackerIDTrajLength = new HashMap<Integer, Long>();
        HashMap<Integer, Long> trackerIDTrajLengthOutput = new HashMap<Integer, Long>();

        String aggregateFunction;
        public CountWindowProcessFunction(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

            minTimestampTrackerID.clear();
            maxTimestampTrackerID.clear();
            trackerIDTrajLength.clear();
            trackerIDTrajLengthOutput.clear();
            Long minTrajLength = Long.MAX_VALUE;
            int minTrajLengthObjID = Integer.MIN_VALUE;
            Long maxTrajLength = Long.MIN_VALUE;
            int maxTrajLengthObjID = Integer.MIN_VALUE;
            Long sumTrajLength = 0L;

            for (Point p : input) {
                Long currMinTimestamp = minTimestampTrackerID.get(p.objID);
                Long currMaxTimestamp = maxTimestampTrackerID.get(p.objID);
                Long minTimestamp = currMinTimestamp;
                Long maxTimestamp = currMaxTimestamp;

                if (currMinTimestamp != null) { // If exists replace else insert
                    if (p.timeStampMillisec < currMinTimestamp) {
                        minTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        minTimestamp = p.timeStampMillisec;
                    }

                    if (p.timeStampMillisec > currMaxTimestamp) {
                        maxTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        maxTimestamp = p.timeStampMillisec;
                    }

                    // Compute the trajectory length and update the map if needed
                    Long currTrajLength = trackerIDTrajLength.get(p.objID);
                    Long trajLength = maxTimestamp - minTimestamp;

                    if (!currTrajLength.equals(trajLength)) { // If exists, replace
                        trackerIDTrajLength.replace(p.objID, trajLength);
                        sumTrajLength -= currTrajLength;
                        sumTrajLength += trajLength;
                    }

                    // Computing MAX Trajectory Length
                    if(trajLength > maxTrajLength){
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = p.objID;
                    }

                    // Computing MIN Trajectory Length
                    if(trajLength < minTrajLength){
                        minTrajLength = trajLength;
                        minTrajLengthObjID = p.objID;
                    }

                } else { // else insert
                    minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    trackerIDTrajLength.put(p.objID, 0L);
                }
            }



            // Tuple5<Key/CellID, #ObjectsInCell, windowStartTime, windowEndTime, Map<TrajId, TrajLength>>
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM")){
                trackerIDTrajLengthOutput.put(Integer.MIN_VALUE, sumTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("AVG")){
                Long avgTrajLength = (Long)Math.round((sumTrajLength * 1.0)/(trackerIDTrajLength.size() * 1.0));
                trackerIDTrajLengthOutput.put(Integer.MIN_VALUE, avgTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){
                trackerIDTrajLengthOutput.put(minTrajLengthObjID, minTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){
                trackerIDTrajLengthOutput.put(maxTrajLengthObjID, maxTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
            }
            else{
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLength));
            }
        }
    }

    // Time Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    public static class TimeWindowProcessFunction extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, String, TimeWindow> {

        // HashMap<TrackerID, timestamp>
        HashMap<Integer, Long> minTimestampTrackerID = new HashMap<Integer, Long>();
        HashMap<Integer, Long> maxTimestampTrackerID = new HashMap<Integer, Long>();
        // HashMap <TrackerID, TrajLength>
        HashMap<Integer, Long> trackerIDTrajLength = new HashMap<Integer, Long>();
        HashMap<Integer, Long> trackerIDTrajLengthOutput = new HashMap<Integer, Long>();

        String aggregateFunction;
        public TimeWindowProcessFunction(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

            minTimestampTrackerID.clear();
            maxTimestampTrackerID.clear();
            trackerIDTrajLength.clear();
            trackerIDTrajLengthOutput.clear();
            Long minTrajLength = Long.MAX_VALUE;
            int minTrajLengthObjID = Integer.MIN_VALUE;
            Long maxTrajLength = Long.MIN_VALUE;
            int maxTrajLengthObjID = Integer.MIN_VALUE;
            Long sumTrajLength = 0L;

            for (Point p : input) {
                Long currMinTimestamp = minTimestampTrackerID.get(p.objID);
                Long currMaxTimestamp = maxTimestampTrackerID.get(p.objID);
                Long minTimestamp = currMinTimestamp;
                Long maxTimestamp = currMaxTimestamp;

                if (currMinTimestamp != null) { // If exists replace else insert
                    if (p.timeStampMillisec < currMinTimestamp) {
                        minTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        minTimestamp = p.timeStampMillisec;
                    }

                    if (p.timeStampMillisec > currMaxTimestamp) {
                        maxTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        maxTimestamp = p.timeStampMillisec;
                    }

                    // Compute the trajectory length and update the map if needed
                    Long currTrajLength = trackerIDTrajLength.get(p.objID);
                    Long trajLength = maxTimestamp - minTimestamp;

                    if (!currTrajLength.equals(trajLength)) {
                        trackerIDTrajLength.replace(p.objID, trajLength);
                        sumTrajLength -= currTrajLength;
                        sumTrajLength += trajLength;
                    }

                    // Computing MAX Trajectory Length
                    if(trajLength > maxTrajLength){
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = p.objID;
                    }

                    // Computing MIN Trajectory Length
                    if(trajLength < minTrajLength){
                        minTrajLength = trajLength;
                        minTrajLengthObjID = p.objID;
                    }



                } else {
                    minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    trackerIDTrajLength.put(p.objID, 0L);
                }
            }

            // Tuple5<Key/CellID, #ObjectsInCell, windowStartTime, windowEndTime, Map<TrajId, TrajLength>>
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM")){
                trackerIDTrajLengthOutput.put(Integer.MIN_VALUE, sumTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("AVG")){
                Long avgTrajLength = (Long)Math.round((sumTrajLength * 1.0)/(trackerIDTrajLength.size() * 1.0));
                trackerIDTrajLengthOutput.put(Integer.MIN_VALUE, avgTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){
                trackerIDTrajLengthOutput.put(minTrajLengthObjID, minTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){
                trackerIDTrajLengthOutput.put(maxTrajLengthObjID, maxTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
            }
            else{
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
        }
    }
}