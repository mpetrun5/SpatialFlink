package GeoFlink.apps;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.RangeQuery;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

public class MovingFeatures implements Serializable {

    //--------------- MOVING FEATURES STAY TIME WITH EMPTY CELLS -----------------//
    public static DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> CellBasedStayTimeWEmptyCells(DataStream<Point> spatialStream, DataStream<Point> sensorSpatialStream, Double sensorRangeRadius, String aggregateFunction, long windowSize, long windowSlideStep, UniformGrid uGrid) {

        // Get sensor cells within radius
        DataStream<String> sensorCellIds = RangeQuery.GetCellsWithinRadius(sensorSpatialStream, sensorRangeRadius, windowSize, windowSlideStep, uGrid);
        //sensorCellIds.print();

        // Get moving object stay time
        DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> windowedCellBasedStayTime = MovingFeatures.CellBasedStayTime(spatialStream, aggregateFunction, "TIME", windowSize, windowSlideStep);
        //windowedCellBasedStayTime.print();


        DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> coGroupedStream = sensorCellIds.coGroup(windowedCellBasedStayTime)
                .where(new KeySelector<String,String>(){
                    @Override
                    public String getKey(String cellId) throws Exception {
                        return cellId;
                    }})
                .equalTo(new KeySelector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>,String>(){
                    @Override
                    public String getKey(Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> stayTime) throws Exception {
                        return stayTime.f0;
                    }})
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new CoGroupFunction<String, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>>() {
                    @Override
                    public void coGroup(Iterable<String> cellIds, Iterable<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> stayTimes, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

                        HashSet<String> cellIdsSet = new HashSet<String>();
                        HashSet<String> neighbouringCellIdsSet = new HashSet<String>();
                        HashSet<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> stayTimesSet = new HashSet<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>>();

                        // Populating cellIdsSet and eliminating duplicates
                        for(String cellId: cellIds){
                            neighbouringCellIdsSet.add(cellId);
                        }

                        // Populating stayTimesSet
                        for(Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> stayTime: stayTimes) {
                            stayTimesSet.add(stayTime);
                        }
                        //System.out.println("cellIdsSet " + cellIdsSet.size() + ", stayTimesSet " + stayTimesSet.size());

                        if(neighbouringCellIdsSet.isEmpty() && stayTimesSet.isEmpty()){
                            // do nothing, no output
                        }
                        else if (neighbouringCellIdsSet.isEmpty()){
                            for(Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> stayTime: stayTimesSet){
                                output.collect(stayTime);
                            }
                        }
                        else if (stayTimesSet.isEmpty()){
                            for(String cellId: neighbouringCellIdsSet){
                                output.collect(Tuple5.of(cellId, 0, Long.MIN_VALUE, Long.MIN_VALUE, new HashMap<Integer, Long>()));
                            }
                        }
                        else { // If both are non-empty

                            // First, inserting the populated cells
                            for (Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> stayTime : stayTimesSet) {
                                output.collect(stayTime);
                                cellIdsSet.add(stayTime.f0);
                            }

                            // Second, inserting the non-populated cells
                            for (String cellId : neighbouringCellIdsSet) {
                                if (!cellIdsSet.contains(cellId)) { // Insert only if not inserted earlier
                                    output.collect(Tuple5.of(cellId, 0, Long.MIN_VALUE, Long.MIN_VALUE, new HashMap<Integer, Long>()));
                                    cellIdsSet.add(cellId);
                                }
                            }
                        }
                    }
                });

        //DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> stayTimeWEmptyCells = coGroupedStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))).apply(new AllWindowFunction<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, TimeWindow>() {
        DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> stayTimeWEmptyCells = coGroupedStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))).apply(new AllWindowFunction<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, TimeWindow>() {

            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> inputStream, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> outputStream) throws Exception {

                HashSet<String> allCellIdsSet = uGrid.getGirdCellsSet();
                //System.out.println("allCellIdsSet size " + allCellIdsSet.size());
                HashSet<String> cellIdsSet = new HashSet<String>();

                for (Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> stayTimeTuple : inputStream) {
                    cellIdsSet.add(stayTimeTuple.f0);
                    outputStream.collect(stayTimeTuple);
                }

                // Collecting the output
                for(String cellId: allCellIdsSet){
                    if(!cellIdsSet.contains(cellId)){
                        // -1 cells added
                        outputStream.collect(Tuple5.of(cellId, -1, Long.MIN_VALUE, Long.MIN_VALUE, new HashMap<Integer, Long>()));
                    }
                }
            }
        });

        return stayTimeWEmptyCells;
    }

    //--------------- MOVING FEATURES STAY TIME -----------------//
    public static DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> CellBasedStayTime(DataStream<Point> pointStream, String aggregateFunction, String windowType, long windowSize, long windowSlideStep) {

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
                    //.window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                    .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                    .process(new TimeWindowProcessFunction(aggregateFunction)).name("Time Windowed Moving Features");

            return tWindowedCellBasedStayTime;
        }
    }


    //--------------- MOVING FEATURES STAY TIME - Angular Grid -----------------//
    public static DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> CellBasedStayTimeAngularGrid(DataStream<Point> pointStream, String aggregateFunction, String windowType, long windowSize, long windowSlideStep) {

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
                    //.window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                    .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))) // Event Time
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
            Long sumTrajLength = 0L; // Maintains sum of all trajectories of a cell

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


    //Time Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    public static class TimeWindowProcessFunction extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, String, TimeWindow> {

        // HashMap<TrackerID, timestamp>
        HashMap<Integer, Long> minTimestampTrackerID = new HashMap<Integer, Long>();
        HashMap<Integer, Long> maxTimestampTrackerID = new HashMap<Integer, Long>();
        // HashMap <TrackerID, TrajLength>
        HashMap<Integer, Long> trackerIDTrajLength = new HashMap<Integer, Long>();

        private ValueState<Long> tuplesCounterValState;


        String aggregateFunction;
        public TimeWindowProcessFunction(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Long> tuplesCounterDescriptor = new ValueStateDescriptor<Long>(
                    "tuplesCounterDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            this.tuplesCounterValState = getRuntimeContext().getState(tuplesCounterDescriptor);
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

            minTimestampTrackerID.clear();
            maxTimestampTrackerID.clear();

            Long tuplesCounterLocal = tuplesCounterValState.value();
            if(tuplesCounterLocal == null)
                tuplesCounterLocal = 0L;

            // Iterate through all the points corresponding to a single grid-cell within the scope of the window
            for (Point p : input) {
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
            }

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

    /*

    //Time Window Process Function
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

            // Iterate through all the points corresponding to a single grid-cell within the scope of the window
            for (Point p : input) {
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

     */
}