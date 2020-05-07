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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;

public class MovingFeatures implements Serializable {


    //--------------- MOVING FEATURES STAY TIME -----------------//
    public static DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> CellBasedStayTime(DataStream<Point> pointStream, int windowSize, int windowSlideStep) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> spatialStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {

                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> windowedCellBasedStayTime = spatialStreamWithTsAndWm
                .keyBy(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
                .process(new ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>, String, TimeWindow>() {

                    // HashMap<TrackerID, timestamp>
                    HashMap<Integer, Long> minTimestampTrackerID = new HashMap<Integer, Long>();
                    HashMap<Integer, Long> maxTimestampTrackerID = new HashMap<Integer, Long>();
                    // HashMap <TrackerID, TrajLength>
                    HashMap<Integer, Long> trackerIDTrajLength = new HashMap<Integer, Long>();

                    @Override
                    // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
                    public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> output) throws Exception {

                        minTimestampTrackerID.clear();
                        maxTimestampTrackerID.clear();
                        trackerIDTrajLength.clear();

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
                                }

                            } else {
                                minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                                maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                                trackerIDTrajLength.put(p.objID, 0L);
                            }
                        }
                        // Tuple5<Key/CellID, #ObjectsInCell, windowStartTime, windowEndTime, Map<TrajId, TrajLength>>
                        output.collect(new Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>(key,
                                trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
                    }
                }).name("Windowed Moving Features");

        return windowedCellBasedStayTime;
    }


}
