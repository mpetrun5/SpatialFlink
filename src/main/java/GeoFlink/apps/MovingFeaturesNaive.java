package GeoFlink.apps;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MovingFeaturesNaive {

    //--------------- MOVING FEATURES STAY TIME -----------------//
    public static DataStream<Tuple4<Long, Long, HashMap<String, Long>, HashMap<String, Integer>>> CellBasedStayTimeNaive(DataStream<Point> pointStream, String aggregateFunction, String windowType, long windowSize, long windowSlideStep) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> spatialStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        // TIME Window
        DataStream<Tuple4<Long, Long, HashMap<String, Long>, HashMap<String, Integer>>> tWindowedCellBasedStayTime = spatialStreamWithTsAndWm
                .keyBy(new objIDKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                //.window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .process(new TimeWindowProcessFunction())
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                //.windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .process(new TimeWindowAllProcessFunction(aggregateFunction));

        return tWindowedCellBasedStayTime;
    }

    //--------------- INCREMENTAL MOVING FEATURES STAY TIME -----------------//
    public static DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> CellBasedStayTimeIncremental(DataStream<Point> pointStream, String aggregateFunction, String windowType, long windowSize, long windowSlideStep) {

        // Filtering out the cells which do not fall into the grid cells
        DataStream<Point> spatialStreamWithoutNullCellID = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                return (p.gridID != null);
            }
        });

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> spatialStreamWithTsAndWm =
                spatialStreamWithoutNullCellID.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> tWindowedCellBasedStayTime = spatialStreamWithTsAndWm
                .keyBy(new MovingFeatures.gridCellKeySelector())
                // //.window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .process(new TimeWindowProcessFunctionIncremental(aggregateFunction)).name("Incremental Time Windowed Moving Features");

        return tWindowedCellBasedStayTime;
    }


    // Incremental Appraoch 1 -Time Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    public static class TimeWindowProcessFunctionIncremental extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, String, TimeWindow> {

        private MapState<Integer, Long> minTimestampTrackerIDMapState;
        private MapState<Integer, Long> maxTimestampTrackerIDMapState;
        private ValueState<Long> lastWindowEndingTimestampValState;
        private ValueState<Long> tuplesCounterValState;

        String aggregateFunction;
        public TimeWindowProcessFunctionIncremental(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Long> minTimestampTrackerIDDescriptor = new MapStateDescriptor<Integer, Long>(
                    "minTimestampTrackerIDDescriptor", // state name
                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            MapStateDescriptor<Integer, Long> maxTimestampTrackerIDDescriptor = new MapStateDescriptor<Integer, Long>(
                    "maxTimestampTrackerIDDescriptor", // state name
                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            ValueStateDescriptor<Long> lastWindowEndingTimestampDescriptor = new ValueStateDescriptor<Long>(
                    "lastWindowEndingTimestampDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            ValueStateDescriptor<Long> tuplesCounterDescriptor = new ValueStateDescriptor<Long>(
                    "tuplesCounterDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            this.minTimestampTrackerIDMapState = getRuntimeContext().getMapState(minTimestampTrackerIDDescriptor);
            this.maxTimestampTrackerIDMapState = getRuntimeContext().getMapState(maxTimestampTrackerIDDescriptor);
            this.lastWindowEndingTimestampValState = getRuntimeContext().getState(lastWindowEndingTimestampDescriptor);
            this.tuplesCounterValState = getRuntimeContext().getState(tuplesCounterDescriptor);
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

            // HashMap<TrackerID, timestamp>
            HashMap<Integer, Long> minTimestampTrackerID = new HashMap<Integer, Long>();
            HashMap<Integer, Long> maxTimestampTrackerID = new HashMap<Integer, Long>();
            // HashMap <TrackerID, TrajLength>
            HashMap<Integer, Long> trackerIDTrajLength = new HashMap<Integer, Long>();

            // Restoring states from MapStates (if exist)
            for (Map.Entry<Integer, Long> entry : minTimestampTrackerIDMapState.entries()) {

                Integer objID = entry.getKey();
                Long minTimestamp = entry.getValue();
                Long maxTimestamp = maxTimestampTrackerIDMapState.get(objID);

                minTimestampTrackerID.put(objID, minTimestamp);
                maxTimestampTrackerID.put(objID, maxTimestamp);
                //System.out.println("objID " + objID + ", " + minTimestamp + ", " + maxTimestamp);
            }

            //System.out.println("winStart " + context.window().getStart() + ", winEnd " + context.window().getEnd());


            // Fetching the lastWinEndingTimestamp from value state variable
            Long lastWinEndingTimestamp = lastWindowEndingTimestampValState.value();
            if(lastWinEndingTimestamp == null)
                lastWinEndingTimestamp = 0L;

            Long tuplesCounterLocal = tuplesCounterValState.value();
            if(tuplesCounterLocal == null)
                tuplesCounterLocal = 0L;

            // Iterate through all the points corresponding to a single grid-cell within the scope of the window
            for (Point p : input) {
                // Filter out points belonging to last window for incremental computing
                //System.out.println("lastWinEndingTimestamp " + lastWinEndingTimestamp + ", p.timeStampMillisec " + p.timeStampMillisec);
                if (p.timeStampMillisec >= lastWinEndingTimestamp) {
                    tuplesCounterLocal++;
                    Long currMinTimestamp = minTimestampTrackerID.get(p.objID);
                    Long currMaxTimestamp = maxTimestampTrackerID.get(p.objID);

                    if (currMinTimestamp != null) { // If exists replace else insert

                        if (p.timeStampMillisec < currMinTimestamp) {
                            minTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        } else if (p.timeStampMillisec > currMaxTimestamp) {
                            maxTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        }

                    } else {
                        minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                        maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    }

                    //System.out.println("objID " + p.objID + ", " + currMinTimestamp + ", " + currMaxTimestamp);
                }
            }

            List<Integer> objIDsToRemove =  new ArrayList<Integer>();
            //Checking for the timestamps not updated in the above window
            for (Map.Entry<Integer, Long> entry : minTimestampTrackerID.entrySet()){

                Integer objID = entry.getKey();
                Long minTimestamp = entry.getValue();
                Long maxTimestamp = maxTimestampTrackerID.get(objID);

                // No update in current window and does not belong to current window, remove it
                // System.out.println("objID " + objID);
                if(maxTimestamp < context.window().getStart()) {
                    objIDsToRemove.add(objID);
                    //minTimestampTrackerID.remove(objID);
                    //maxTimestampTrackerID.remove(objID);
                }
                else if(minTimestamp < context.window().getStart()) { // update found in current window but minTimestamp does not belong to current window
                    minTimestampTrackerID.replace(objID, context.window().getStart());  // update minTimestampTrackerID to current window starting timestamp
                }
            }
            // Removing the expired objects identified in above loop
            for (Integer objID: objIDsToRemove){
                minTimestampTrackerID.remove(objID);
                maxTimestampTrackerID.remove(objID);
            }

            // Storing states to MapStates
            minTimestampTrackerIDMapState.clear();
            maxTimestampTrackerIDMapState.clear();
            minTimestampTrackerIDMapState.putAll(minTimestampTrackerID);
            maxTimestampTrackerIDMapState.putAll(maxTimestampTrackerID);
            lastWindowEndingTimestampValState.update(context.window().getEnd());
            tuplesCounterValState.update(tuplesCounterLocal);

            // Generating Output based on aggregateFunction variable
            // Tuple5<Key/CellID, #ObjectsInCell, windowStartTime, windowEndTime, Map<TrajId, TrajLength>>
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){

                trackerIDTrajLength.clear();
                for (Map.Entry<Integer, Long> entry : minTimestampTrackerID.entrySet()) {
                    Integer objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    trackerIDTrajLength.put(objID, (currMaxTimestamp-currMinTimestamp));
                }
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        minTimestampTrackerID.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM") || this.aggregateFunction.equalsIgnoreCase("AVG")){

                trackerIDTrajLength.clear();
                Long sumTrajLength = 0L;



                for (Map.Entry<Integer, Long> entry : minTimestampTrackerID.entrySet()) {
                    Integer objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    sumTrajLength += (currMaxTimestamp-currMinTimestamp);
                }

                if(this.aggregateFunction.equalsIgnoreCase("SUM"))
                {
                    trackerIDTrajLength.put(Integer.MIN_VALUE, sumTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                            minTimestampTrackerID.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
                }
                else // AVG
                {
                    Long avgTrajLength = (Long)Math.round((sumTrajLength * 1.0)/(minTimestampTrackerID.size() * 1.0));
                    trackerIDTrajLength.put(Integer.MIN_VALUE, avgTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                            minTimestampTrackerID.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){

                Long minTrajLength = Long.MAX_VALUE;
                int minTrajLengthObjID = Integer.MIN_VALUE;
                trackerIDTrajLength.clear();

                for (Map.Entry<Integer, Long> entry : minTimestampTrackerID.entrySet()) {
                    Integer objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    Long trajLength = currMaxTimestamp-currMinTimestamp;

                    if(trajLength < minTrajLength){
                        minTrajLength = trajLength;
                        minTrajLengthObjID = objID;
                    }
                }

                trackerIDTrajLength.put(minTrajLengthObjID, minTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        minTimestampTrackerID.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){

                Long maxTrajLength = Long.MIN_VALUE;
                int maxTrajLengthObjID = Integer.MIN_VALUE;
                trackerIDTrajLength.clear();

                for (Map.Entry<Integer, Long> entry : minTimestampTrackerID.entrySet()) {
                    Integer objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    Long trajLength = currMaxTimestamp-currMinTimestamp;

                    if(trajLength > maxTrajLength){
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = objID;
                    }
                }

                trackerIDTrajLength.put(maxTrajLengthObjID, maxTrajLength);
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        minTimestampTrackerID.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
            else{
                trackerIDTrajLength.clear();
                for (Map.Entry<Integer, Long> entry : minTimestampTrackerID.entrySet()) {
                    Integer objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    trackerIDTrajLength.put(objID, (currMaxTimestamp-currMinTimestamp));
                }
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
        }
    }


    // Incremental Appraoch 2 -Time Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    /*public static class TimeWindowProcessFunctionIncremental extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, String, TimeWindow> {



        private MapState<Integer, Long> minTimestampTrackerIDMapState;
        private MapState<Integer, Long> maxTimestampTrackerIDMapState;
        private MapState<Integer, Long> trackerIDTrajLengthMapState;
        private ValueState<Long> lastWindowEndingTimestamp;

        String aggregateFunction;
        public TimeWindowProcessFunctionIncremental(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Long> minTimestampTrackerIDDescriptor = new MapStateDescriptor<Integer, Long>(
                    "minTimestampTrackerIDDescriptor", // state name
                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            MapStateDescriptor<Integer, Long> maxTimestampTrackerIDDescriptor = new MapStateDescriptor<Integer, Long>(
                    "maxTimestampTrackerIDDescriptor", // state name
                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            MapStateDescriptor<Integer, Long> trackerIDTrajLengthDescriptor = new MapStateDescriptor<Integer, Long>(
                    "trackerIDTrajLengthDescriptor", // state name
                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            ValueStateDescriptor<Long> lastWindowEndingTimestampDescriptor = new ValueStateDescriptor<Long>(
                    "lastWindowEndingTimestampDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            this.minTimestampTrackerIDMapState = getRuntimeContext().getMapState(minTimestampTrackerIDDescriptor);
            this.maxTimestampTrackerIDMapState = getRuntimeContext().getMapState(maxTimestampTrackerIDDescriptor);
            this.trackerIDTrajLengthMapState = getRuntimeContext().getMapState(trackerIDTrajLengthDescriptor);
            this.lastWindowEndingTimestamp = getRuntimeContext().getState(lastWindowEndingTimestampDescriptor);
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

            // HashMap<TrackerID, timestamp>
            HashMap<Integer, Long> minTimestampTrackerID = new HashMap<Integer, Long>();
            HashMap<Integer, Long> maxTimestampTrackerID = new HashMap<Integer, Long>();
            // HashMap <TrackerID, TrajLength>
            HashMap<Integer, Long> trackerIDTrajLength = new HashMap<Integer, Long>();
            HashMap<Integer, Long> trackerIDTrajLengthOutput = new HashMap<Integer, Long>();

            Long minTrajLength = Long.MAX_VALUE;
            int minTrajLengthObjID = Integer.MIN_VALUE;
            Long maxTrajLength = Long.MIN_VALUE;
            int maxTrajLengthObjID = Integer.MIN_VALUE;
            Long sumTrajLength = 0L;

            // Restoring states from MapStates
            for (Map.Entry<Integer, Long> entry : minTimestampTrackerIDMapState.entries()) {

                Integer objID = entry.getKey();
                Long minTimestamp = entry.getValue();

                Long maxTimestamp = maxTimestampTrackerIDMapState.get(objID);
                Long cellStayTime = trackerIDTrajLengthMapState.get(objID);

                minTimestampTrackerID.put(objID, minTimestamp);
                maxTimestampTrackerID.put(objID, maxTimestamp);
                trackerIDTrajLength.put(objID, cellStayTime);
            }

            Long lastWinEndingTimestamp = lastWindowEndingTimestamp.value();

            // Iterate through all the points corresponding to a single grid-cell within the scope of the window
            for (Point p : input) {
                // Filter out points belonging to last window for incremental computing
                if (p.timeStampMillisec >= lastWinEndingTimestamp)
                {
                    Long currMinTimestamp = minTimestampTrackerID.get(p.objID);
                    Long currMaxTimestamp = maxTimestampTrackerID.get(p.objID);
                    Long minTimestamp = currMinTimestamp;
                    Long maxTimestamp = currMaxTimestamp;

                    //System.out.println("point timestamp " + p.timeStampMillisec + " Window bounds: " + context.window().getStart() + ", " + context.window().getEnd());

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
                        if (trajLength > maxTrajLength) {
                            maxTrajLength = trajLength;
                            maxTrajLengthObjID = p.objID;
                        }

                        // Computing MIN Trajectory Length
                        if (trajLength < minTrajLength) {
                            minTrajLength = trajLength;
                            minTrajLengthObjID = p.objID;
                        }

                    } else {
                        minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                        maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                        trackerIDTrajLength.put(p.objID, 0L);
                    }
                }
            }

            //Checking for the timestamps not updated in the above window
            for (Map.Entry<Integer, Long> entry : minTimestampTrackerIDMapState.entries()) {

                Integer objID = entry.getKey();
                Long minTimestamp = entry.getValue();
                Long maxTimestamp = maxTimestampTrackerIDMapState.get(objID);

                // No update in current window and does not belong to current window, remove it
                if(maxTimestamp < context.window().getStart()) {
                    minTimestampTrackerID.remove(objID);
                    maxTimestampTrackerID.remove(objID);
                    trackerIDTrajLength.remove(objID);
                }
                else if(minTimestamp < context.window().getStart()) { // update found in current window but minTimestamp does not belong to current window
                    minTimestampTrackerID.replace(objID, context.window().getStart());

                    // Compute the trajectory length and update the map if needed
                    Long currTrajLength = trackerIDTrajLength.get(objID);
                    Long trajLength = maxTimestamp - context.window().getStart();

                    if (!currTrajLength.equals(trajLength)) {
                        trackerIDTrajLength.replace(objID, trajLength);
                        sumTrajLength -= currTrajLength;
                        sumTrajLength += trajLength;
                    }

                    // Computing MAX Trajectory Length
                    if (trajLength > maxTrajLength) {
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = objID;
                    }

                    // Computing MIN Trajectory Length
                    if (trajLength < minTrajLength) {
                        minTrajLength = trajLength;
                        minTrajLengthObjID = objID;
                    }
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





    }*/

    // Key selector
    public static class objIDKeySelector implements KeySelector<Point,Integer> {
        @Override
        public Integer getKey(Point p) throws Exception {
            return p.objID;
        }
    }

    //ProcessWindowFunction <IN, OUT, KEY, WINDOW>
    public static class TimeWindowProcessFunction extends ProcessWindowFunction<Point, Tuple2<Integer, HashMap<String, Long>>, Integer, TimeWindow> {

        // HashMap<gridID, timestamp> - to maintain cell wise min-max timestamps
        HashMap<String, Long> gridCellMinTimestamp = new HashMap<String, Long>();
        HashMap<String, Long> gridCellMaxTimestamp = new HashMap<String, Long>();
        // HashMap <gridID, TrajLength>
        HashMap<String, Long> gridCellTrajLength = new HashMap<String, Long>();

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(Integer key, Context context, Iterable<Point> inputTrajectory, Collector<Tuple2<Integer, HashMap<String, Long>>> output) throws Exception {

            gridCellMinTimestamp.clear();
            gridCellMaxTimestamp.clear();
            gridCellTrajLength.clear();

            for (Point p : inputTrajectory) {

                Long currMinTimestamp = gridCellMinTimestamp.get(p.gridID);
                Long currMaxTimestamp = gridCellMaxTimestamp.get(p.gridID);
                Long minTimestamp = currMinTimestamp;
                Long maxTimestamp = currMaxTimestamp;


                if (currMinTimestamp != null) { // If exists replace else insert
                    if (p.timeStampMillisec < currMinTimestamp) {
                        gridCellMinTimestamp.replace(p.gridID, p.timeStampMillisec);
                        minTimestamp = p.timeStampMillisec;
                    }

                    if (p.timeStampMillisec > currMaxTimestamp) {
                        gridCellMaxTimestamp.replace(p.gridID, p.timeStampMillisec);
                        maxTimestamp = p.timeStampMillisec;
                    }

                    // Compute the trajectory length and update the map if needed
                    Long currTrajLength = gridCellTrajLength.get(p.gridID);
                    Long trajLength = maxTimestamp - minTimestamp;

                    if (!currTrajLength.equals(trajLength)) {
                        gridCellTrajLength.replace(p.gridID, trajLength);
                    }

                } else { // If a cell does not exist in hashmap, create it
                    gridCellMinTimestamp.put(p.gridID, p.timeStampMillisec);
                    gridCellMaxTimestamp.put(p.gridID, p.timeStampMillisec);
                    gridCellTrajLength.put(p.gridID, 0L);
                }
            }

            // Tuple2<Key/ObjID/TrajId, Map<cellID, TrajLength>>
            output.collect(new Tuple2<Integer, HashMap<String, Long>>(key, gridCellTrajLength));
        }
    }

    //ProcessWindowFunction <IN, OUT, WINDOW>
    public static class TimeWindowAllProcessFunction extends ProcessAllWindowFunction<Tuple2<Integer, HashMap<String, Long>>, Tuple4<Long, Long, HashMap<String, Long>, HashMap<String, Integer>>, TimeWindow> {

        // HashMap<gridID, timestamp>
        HashMap<String, Long> gridCellStayTime = new HashMap<String, Long>();
        HashMap<String, Integer> gridCellNumObjects = new HashMap<String, Integer>();
        String aggregateFunction;

        public TimeWindowAllProcessFunction(String aggregateFunction) {
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void process(Context context, Iterable<Tuple2<Integer, HashMap<String, Long>>> inputIterable, Collector<Tuple4<Long, Long, HashMap<String, Long>, HashMap<String, Integer>>> output) throws Exception {

            // ObjectID, HashMap <cellID, stayTime>
            // Main loop consisting of all the inputs from different nodes
            for (Tuple2<Integer, HashMap<String, Long>> inputTuple : inputIterable) {

                HashMap<String, Long> cellBasedStayTimeMap = inputTuple.f1;
                if (cellBasedStayTimeMap.size() > 0) {

                    // Each cellBasedStayTimeMap contains cells corresponding to one trajectory
                    for (Map.Entry<String, Long> entry : cellBasedStayTimeMap.entrySet()) {

                        String gridID = entry.getKey();
                        Long cellStayTime = entry.getValue();

                        Long cellStayTimeCurrent = gridCellStayTime.get(gridID);

                        if (cellStayTimeCurrent == null) { // If cell does not exist in gridCellStayTime, add it
                            gridCellStayTime.put(gridID, cellStayTime);
                            gridCellNumObjects.put(gridID, 1);
                        } else { // if already exist

                            //Number of objects in a cell
                            Integer cellNumObjects = gridCellNumObjects.get(gridID);
                            gridCellNumObjects.replace(gridID, (cellNumObjects + 1));

                            if (this.aggregateFunction.equalsIgnoreCase("SUM") || this.aggregateFunction.equalsIgnoreCase("AVG")) {
                                cellStayTimeCurrent += cellStayTime;
                                gridCellStayTime.replace(gridID, cellStayTimeCurrent);
                            } else if (this.aggregateFunction.equalsIgnoreCase("MIN")) {

                                if (cellStayTime < cellStayTimeCurrent) {
                                    gridCellStayTime.replace(gridID, cellStayTime);
                                }
                            } else if (this.aggregateFunction.equalsIgnoreCase("MAX")) {
                                if (cellStayTime > cellStayTimeCurrent) {
                                    gridCellStayTime.replace(gridID, cellStayTime);
                                }
                            } else { // Default: SUM
                                cellStayTimeCurrent += cellStayTime;
                                gridCellStayTime.replace(gridID, cellStayTimeCurrent);
                            }
                        }
                    }
                }
            }

            if (this.aggregateFunction.equalsIgnoreCase("AVG")) {

                for (Map.Entry<String, Long> entry : gridCellStayTime.entrySet()) {

                    String gridID = entry.getKey();
                    Long cellStayTime = entry.getValue();
                    Integer cellNumObjects = gridCellNumObjects.get(gridID);
                    Long avgStayTime = (Long) Math.round((cellStayTime * 1.0) / (cellNumObjects * 1.0));
                    gridCellStayTime.replace(gridID, avgStayTime);
                }
            }

            // Tuple4<Window start, Window end, mapOfStayTimeWRTCells, mapOfObjCountWRTCells >
            output.collect(new Tuple4<Long, Long, HashMap<String, Long>, HashMap<String, Integer>>(context.window().getStart(), context.window().getEnd(), gridCellStayTime, gridCellNumObjects));
        }
    }
}
