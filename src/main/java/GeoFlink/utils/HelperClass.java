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

package GeoFlink.utils;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HelperClass {

    private static final double mEarthRadius = 6371008.7714; // Earth radius in meters


    // startingPoint and bearing are in degrees
    // Source: https://www.movable-type.co.uk/scripts/latlong.html
    public static Coordinate computeDestinationPoint(Coordinate startingPoint, double angleFromTrueNorth, double distanceInMeters)
    {
        double lon1 = Math.toRadians(startingPoint.getX());
        double lat1 = Math.toRadians(startingPoint.getY());
        double bearing = Math.toRadians(angleFromTrueNorth);
        double cosOfDist = Math.cos(distanceInMeters/mEarthRadius);
        double sinOfDist = Math.sin(distanceInMeters/mEarthRadius);
        double cosOfLat1 = Math.cos(lat1);
        double sinOfLat1 = Math.sin(lat1);

        double lat2 = Math.asin(sinOfLat1 * cosOfDist + cosOfLat1 * sinOfDist * Math.cos(bearing) );
        double lon2 = lon1 + Math.atan2(Math.sin(bearing) * sinOfDist * cosOfLat1, cosOfDist - sinOfLat1 * Math.sin(lat2));

        return new Coordinate(Math.toDegrees(lon2), Math.toDegrees(lat2));
    }

    // return a string padded with zeroes to make the string equivalent to desiredStringLength
    public static String padLeadingZeroesToInt(int cellIndex, int desiredStringLength)
    {
        return String.format("%0"+ Integer.toString(desiredStringLength) +"d", cellIndex);
    }

    // return an integer by removing the leading zeroes from the string
    public static int removeLeadingZeroesFromString(String str)
    {
        return Integer.parseInt(str.replaceFirst("^0+(?!$)", ""));
    }

    public static boolean pointWithinQueryRange(ArrayList<Integer> pointCellIndices, ArrayList<Integer> queryCellIndices, int neighboringLayers){

        if((pointCellIndices.get(0) >= queryCellIndices.get(0) - neighboringLayers) && (pointCellIndices.get(0) <= queryCellIndices.get(0) + neighboringLayers) && (pointCellIndices.get(1) >= queryCellIndices.get(1) - neighboringLayers) && (pointCellIndices.get(1) <= queryCellIndices.get(1) + neighboringLayers)){
            return true;
        }
        else{
            return false;
        }
    }

    public static ArrayList<Integer> getIntCellIndices(String cellID)
    {
        ArrayList<Integer> cellIndices = new ArrayList<Integer>();

        //substring(int startIndex, int endIndex): endIndex is excluded
        String cellIDX = cellID.substring(0,5);
        String cellIDY = cellID.substring(5);

        cellIndices.add(HelperClass.removeLeadingZeroesFromString(cellIDX));
        cellIndices.add(HelperClass.removeLeadingZeroesFromString(cellIDY));

        return cellIndices;
    }

    public static List<Tuple2<Double, Double>> getCellCoordinates(ArrayList<Integer> cellIndices, UniformGrid uGrid)
    {
        //ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(cellId);
        List<Tuple2<Double, Double>> cellCoordinates = new ArrayList<Tuple2<Double, Double>>();
        double minX = uGrid.getMinX();
        double minY = uGrid.getMinY();
        double cellLength = uGrid.getCellLength();

        double cellMinX = minX + (cellLength * cellIndices.get(0));
        double cellMinY = minY + (cellLength * cellIndices.get(1));

        double cellMaxX = minX + (cellLength * (cellIndices.get(0) + 1));
        double cellMaxY = minY + (cellLength * (cellIndices.get(1) + 1));

        cellCoordinates.add(new Tuple2<>(cellMinX, cellMinY));
        cellCoordinates.add(new Tuple2<>(cellMinX, cellMaxY));
        cellCoordinates.add(new Tuple2<>(cellMaxX, cellMaxY));
        cellCoordinates.add(new Tuple2<>(cellMaxX, cellMinY));
        cellCoordinates.add(new Tuple2<>(cellMinX, cellMinY));

        return cellCoordinates;
    }

    public static List<Tuple2<Double, Double>> getCellCoordinates(String cellId, UniformGrid uGrid)
    {
        ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(cellId);
        List<Tuple2<Double, Double>> cellCoordinates = new ArrayList<Tuple2<Double, Double>>();
        double minX = uGrid.getMinX();
        double minY = uGrid.getMinY();
        double cellLength = uGrid.getCellLength();

        double cellMinX = minX + (cellLength * cellIndices.get(0));
        double cellMinY = minY + (cellLength * cellIndices.get(1));

        double cellMaxX = minX + (cellLength * (cellIndices.get(0) + 1));
        double cellMaxY = minY + (cellLength * (cellIndices.get(1) + 1));

        cellCoordinates.add(new Tuple2<>(cellMinX, cellMinY));
        cellCoordinates.add(new Tuple2<>(cellMinX, cellMaxY));
        cellCoordinates.add(new Tuple2<>(cellMaxX, cellMaxY));
        cellCoordinates.add(new Tuple2<>(cellMaxX, cellMinY));
        cellCoordinates.add(new Tuple2<>(cellMinX, cellMinY));

        return cellCoordinates;
    }

    public static Integer getCellLayerWRTQueryCell(String queryCellID, String cellID)
    {
        ArrayList<Integer> queryCellIndices = getIntCellIndices(queryCellID);
        ArrayList<Integer> cellIndices = getIntCellIndices(cellID);
        Integer cellLayer;

        if((queryCellIndices.get(0) == cellIndices.get(0)) && (queryCellIndices.get(1) == cellIndices.get(1))) {
            return 0; // cell layer is 0
        }
        else if ( Math.abs(queryCellIndices.get(0) - cellIndices.get(0)) == 0){
            return Math.abs(queryCellIndices.get(1) - cellIndices.get(1));
        }
        else if ( Math.abs(queryCellIndices.get(1) - cellIndices.get(1)) == 0){
            return Math.abs(queryCellIndices.get(0) - cellIndices.get(0));
        }
        else{
            return Math.max(Math.abs(queryCellIndices.get(0) - cellIndices.get(0)), Math.abs(queryCellIndices.get(1) - cellIndices.get(1)));
        }
    }

    public static double computeEuclideanDistance(Double lon, Double lat, Double lon1, Double lat1) {

        return Math.sqrt( Math.pow((lat1 - lat),2) + Math.pow((lon1 - lon),2));
    }

    public static double computeHaverSine(Double lon1, Double lat1, Double lon2, Double lat2) {
        Double rLat1 = Math.toRadians(lat1);
        Double rLat2 = Math.toRadians(lat2);
        Double dLon=Math.toRadians(lon2-lon1);
        Double distance= Math.acos(Math.sin(rLat1)*Math.sin(rLat2) + Math.cos(rLat1)*Math.cos(rLat2) * Math.cos(dLon)) * mEarthRadius;
        return distance;
    }


    public static class checkExitControlTuple implements FilterFunction<ObjectNode> {
        @Override
        public boolean filter(ObjectNode json) throws Exception {
            String objType = json.get("value").get("geometry").get("type").asText();
            if (objType.equals("control")) {
                try {
                    throw new IOException();
                } finally {}

            }
            else return true;
        }
    }

    public static DataStream<Point> getUnifiedStream(List<DataStream<Point>> spatialStreams){

        DataStream<Point> spatialStream;

        if(spatialStreams.size() == 2)
            spatialStream = spatialStreams.get(0).union(spatialStreams.get(1));
        else if (spatialStreams.size() == 3){
            spatialStream = spatialStreams.get(0).union(spatialStreams.get(1), spatialStreams.get(2));
        }
        else if (spatialStreams.size() == 4){
            spatialStream = spatialStreams.get(0).union(spatialStreams.get(1), spatialStreams.get(2), spatialStreams.get(3));
        }
        else if (spatialStreams.size() == 5){
            spatialStream = spatialStreams.get(0).union(spatialStreams.get(1), spatialStreams.get(2), spatialStreams.get(3), spatialStreams.get(4));
        }
        else
            spatialStream = spatialStreams.get(0);

        return spatialStream;
    }

}
