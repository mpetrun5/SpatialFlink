package GeoFlink.streamCleaning;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialStreams.SpatialStream;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Coordinate;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class anomalyDetection {

    public static DataStream<Tuple2<String, String>> speedConstraint(DataStream inputStream, double minSpeed, double maxSpeed, long speedWinSizeMillisec) {

        DataStream outputStream = inputStream.map(new SpeedConstraintMap(minSpeed, maxSpeed, speedWinSizeMillisec));
        return  outputStream;
    }

    public static class SpeedConstraintMap extends RichMapFunction<ObjectNode, ObjectNode> {

        double minSpeedConstraint;
        double maxSpeedConstraint;
        long speedConstraintWinSizeMillisec;

        private ValueState<Long> lastTimestampVState;
        private ValueState<Double> lastLongitudeVState;
        private ValueState<Double> lastLatitudeVState;

        //ctor
        public  SpeedConstraintMap() {};
        public  SpeedConstraintMap(double minSpeed, double maxSpeed, long speedWinSizeMillisec)
        {
            this.minSpeedConstraint = minSpeed;
            this.maxSpeedConstraint = maxSpeed;
            this.speedConstraintWinSizeMillisec = speedWinSizeMillisec;
        };

        @Override
        public void open(Configuration config) {

            ValueStateDescriptor<Long> lastTimestampDescriptor = new ValueStateDescriptor<Long>(
                    "lastTimestampDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            ValueStateDescriptor<Double> lastLongitudeDescriptor = new ValueStateDescriptor<Double>(
                    "lastLongitudeDescriptor", // state name
                    BasicTypeInfo.DOUBLE_TYPE_INFO);

            ValueStateDescriptor<Double> lastLatitudeDescriptor = new ValueStateDescriptor<Double>(
                    "lastLatitudeDescriptor", // state name
                    BasicTypeInfo.DOUBLE_TYPE_INFO);

            this.lastTimestampVState = getRuntimeContext().getState(lastTimestampDescriptor);
            this.lastLongitudeVState = getRuntimeContext().getState(lastLongitudeDescriptor);
            this.lastLatitudeVState = getRuntimeContext().getState(lastLatitudeDescriptor);
        }

        @Override
        public ObjectNode map(ObjectNode jsonObj) throws Exception {

            Long lastTimestamp;
            Double lastLongitude;
            Double lastLatitude;

            // Fetching the value state variables
            lastTimestamp = lastTimestampVState.value();
            lastLongitude = lastLongitudeVState.value();
            lastLatitude = lastLatitudeVState.value();

            /*
            if(lastTimestamp == null)
                lastTimestamp = Long.MIN_VALUE;

            if(lastLongitude == null)
                lastLongitude = Double.MIN_VALUE;

            if(lastLatitude == null)
                lastLatitude = Double.MIN_VALUE;
             */

            // Parsing the JSON Element Node
            DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            int oID = jsonObj.get("value").get("properties").get("oID").asInt();
            Date dateTime = simpleDateFormat.parse(jsonObj.get("value").get("properties").get("timestamp").asText());
            long currTimestamp = dateTime.getTime();
            double currLongitude = jsonObj.get("value").get("geometry").get("coordinates").get(0).asDouble();
            double currLatitude = jsonObj.get("value").get("geometry").get("coordinates").get(1).asDouble();


            double speedChange;
            double haverSineDist;
            boolean withinSpeedLimit;


            // The case of first element with no past info
            if (lastTimestamp == null) {
                speedChange = Double.MIN_VALUE;
                withinSpeedLimit = true;
            }else{
                //1D speed change
                //speedChange = computeSpeedChange(lastLatitude, currLatitude, lastTimestamp, currTimestamp, speedConstraintWinSizeMillisec);
                //2D (spatial) speed change
                speedChange = HelperClass.computeHaverSine(lastLongitude, lastLatitude, currLongitude, currLatitude);

                withinSpeedLimit = withinSpeedLimit(speedChange, minSpeedConstraint, maxSpeedConstraint);
            }

            // Updating the state variables
            lastTimestampVState.update(currTimestamp);
            lastLongitudeVState.update(currLongitude);
            lastLatitudeVState.update(currLatitude);

            //return Tuple6.of(oID, currTimestamp, currLongitude, currLatitude, speedChange, withinSpeedLimit);
            return null;
        }
    }


    static double computeSpeedChange(double prevPoint, double currPoint, long lastTimestamp, long currTimestamp, long speedConstraintWinSizeMillisec)
    {
        Long timestampDiff = currTimestamp - lastTimestamp;

        if (timestampDiff > speedConstraintWinSizeMillisec){
            return Double.MIN_VALUE;
        }
        else{
            return (currPoint - prevPoint)/timestampDiff;
        }
    }

    static double computeSpeedChange(double distance, long lastTimestamp, long currTimestamp, long speedConstraintWinSizeMillisec)
    {
        Long timestampDiff = currTimestamp - lastTimestamp;

        if (timestampDiff > speedConstraintWinSizeMillisec){
            return Double.MIN_VALUE;
        }
        else{
            return distance/timestampDiff;
        }
    }

    static boolean withinSpeedLimit(double speedChange, double minSpeed, double maxSpeed){

        if(speedChange > maxSpeed || speedChange < minSpeed){
            return false;
        }
        else return true;

    }




}
