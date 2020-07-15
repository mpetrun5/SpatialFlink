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

package GeoFlink.spatialIndices;

import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.HelperClass;
import org.apache.commons.collections.list.TreeList;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.*;

public class UniformGrid implements Serializable {

    double minX;     //X - East-West longitude
    double maxX;
    double minY;     //Y - North-South latitude
    double maxY;

    final private int CELLINDEXSTRLENGTH = 5;
    double cellLength;
    int numGridPartitions;
    double cellLengthMeters;
    HashSet<String> girdCellsSet = new HashSet<String>();
    STRtree gridTreeIndex;
    //List<Polygon> cellList = new ArrayList<>();
    Map<Polygon, String> cellToKeyMap;
    boolean isAngularGrid = false;

    //TODO: Remove variable cellLengthMeters (Deprecated)
    // Constructors

    // Angular Grid
    // Since the distance function makes use of earth radius in meters, the cell length is also in meters
    public UniformGrid(JSONArray gridPointCoordinatesArr, double angleInDegree, double cellLengthInMeters, int numRows, int numColumns)
    {
        // setting the flag of angular grid to true
        this.isAngularGrid = true;

        // Here p is the starting point of the grid
        double x = gridPointCoordinatesArr.getDouble(0);
        double y = gridPointCoordinatesArr.getDouble(1);
        //System.out.println("x: " + x + ", y: " + y);

        GeometryFactory geofact = new GeometryFactory();
        org.locationtech.jts.geom.Point p = geofact.createPoint(new Coordinate(x, y));

        int numGridDivisions = Math.max(numRows, numColumns);
        this.cellLength = cellLengthInMeters;
        this.cellLengthMeters = cellLengthInMeters;

        if(numGridDivisions < 1)
            this.numGridPartitions = 1;
        else
            this.numGridPartitions = numGridDivisions;

        //System.out.println("cellLengthMeters " + cellLengthMeters);
        // Populating the girdCellset - contains all the cells in the grid
        populateGridCells(p.getCoordinate(), angleInDegree);
    }


    public UniformGrid(double cellLengthDesired, JSONArray inputCoordinatesArr)
    {
        initializeGridCoordinates(inputCoordinatesArr);
        adjustCoordinatesForSquareGrid();

        double gridLengthMeters = HelperClass.computeHaverSine(minX, minY, maxX, minY);
        double numGridRows = gridLengthMeters/cellLengthDesired;

        if(numGridRows < 1)
            this.numGridPartitions = 1;
        else
            this.numGridPartitions = (int)Math.ceil(numGridRows);

        this.cellLength = (maxX - minX) / this.numGridPartitions;
        this.cellLengthMeters = HelperClass.computeHaverSine(minX, minY, minX + cellLength, minY);
        //System.out.println("cellLengthMeters " + cellLengthMeters);

        // Populating the girdCellset - contains all the cells in the grid
        populateGridCells();
    }

    public UniformGrid(double cellLengthDesired, double minLongitude, double maxLongitude, double minLatitude, double maxLatitude)
    {
        this.minX = minLongitude;     //X - East-West longitude
        this.maxX = maxLongitude;
        this.minY = minLatitude;     //Y - North-South latitude
        this.maxY = maxLatitude;

        adjustCoordinatesForSquareGrid();

        double gridLengthMeters = HelperClass.computeHaverSine(minX, minY, maxX, minY);
        double numGridRows = gridLengthMeters/cellLengthDesired;

        if(numGridRows < 1)
            this.numGridPartitions = 1;
        else
            this.numGridPartitions = (int)Math.ceil(numGridRows);

        this.cellLength = (maxX - minX) / this.numGridPartitions;
        this.cellLengthMeters = HelperClass.computeHaverSine(minX, minY, minX + cellLength, minY);
        //System.out.println("cellLengthMeters " + cellLengthMeters);

        // Populating the girdCellset - contains all the cells in the grid
        populateGridCells();

        //System.out.println("gridLengthMeters " + gridLengthMeters);
        //System.out.println("numGridRows " + numGridRows);
        //System.out.println("numGridPartitions: " + numGridPartitions);
        //System.out.println("cellLength: " + cellLength);
    }

    public UniformGrid(int uniformGridRows, double minLongitude, double maxLongitude, double minLatitude, double maxLatitude)
    {
        this.minX = minLongitude;     //X - East-West longitude
        this.maxX = maxLongitude;
        this.minY = minLatitude;     //Y - North-South latitude
        this.maxY = maxLatitude;

        this.numGridPartitions = uniformGridRows;

        adjustCoordinatesForSquareGrid();

        this.cellLength = (maxX - minX) / uniformGridRows;
        //System.out.println("cellLength: " + cellLength);
        this.cellLengthMeters = HelperClass.computeHaverSine(minX, minY, minX + cellLength, minY);

        // Populating the girdCellset - contains all the cells in the grid
        populateGridCells();
    }

    private void initializeGridCoordinates(JSONArray inputCoordinatesArr){

        double minLongitude = Double.MAX_VALUE;
        double minLatitude = Double.MAX_VALUE;
        double maxLongitude= Double.MIN_VALUE;;
        double maxLatitude = Double.MIN_VALUE;;

        for (int i = 0; i < inputCoordinatesArr.length(); i++)
        {
            JSONArray inputCoordinatesPoint = inputCoordinatesArr.getJSONArray(i);
            {
                if (minLongitude > inputCoordinatesPoint.getDouble(0))
                    minLongitude = inputCoordinatesPoint.getDouble(0);

                if (maxLongitude < inputCoordinatesPoint.getDouble(0))
                    maxLongitude = inputCoordinatesPoint.getDouble(0);

                if (minLatitude > inputCoordinatesPoint.getDouble(1))
                    minLatitude = inputCoordinatesPoint.getDouble(1);

                if (maxLatitude < inputCoordinatesPoint.getDouble(1))
                    maxLatitude = inputCoordinatesPoint.getDouble(1);
            }
        }

        this.minX = minLongitude;     //X - East-West longitude
        this.maxX = maxLongitude;
        this.minY = minLatitude;     //Y - North-South latitude
        this.maxY = maxLatitude;

        //System.out.println("minLongitude " + minLongitude);
        //System.out.println("maxLongitude " + maxLongitude);
        //System.out.println("minLatitude " + minLatitude);
        //System.out.println("maxLatitude " + maxLatitude);
    }

    public void adjustCoordinatesForSquareGrid(){
        double xAxisDiff = this.maxX - this.minX;
        double yAxisDiff = this.maxY - this.minY;

        // Adjusting coordinates to make square grid cells
        if(xAxisDiff > yAxisDiff)
        {
            double diff = xAxisDiff - yAxisDiff;
            this.maxY += diff / 2;
            this.minY -= diff / 2;
        }
        else if(yAxisDiff > xAxisDiff)
        {
            double diff = yAxisDiff - xAxisDiff;
            this.maxX += diff / 2;
            this.minX -= diff / 2;
        }
    }

    private void populateGridCells(){
        for (int i = 0; i < this.numGridPartitions; i++) {
            for (int j = 0; j < this.numGridPartitions; j++) {
                String cellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                girdCellsSet.add(cellKey);
                //System.out.println("cellKey " + cellKey);
            }
        }
    }

    private void populateGridCells(Coordinate startingPoint, double angleFromTrueNorth){

        this.gridTreeIndex = new STRtree(this.numGridPartitions*this.numGridPartitions);
        this.cellToKeyMap = new TreeMap<Polygon, String>();
        Coordinate nextStartingPoint = startingPoint;

        // Populating the grid
        for (int i = 0; i < this.numGridPartitions; i++) {
            for (int j = 0; j < this.numGridPartitions; j++) {

                Coordinate destinationPoint1 = HelperClass.computeDestinationPoint(startingPoint, angleFromTrueNorth, this.cellLength);
                Coordinate destinationPoint2 = HelperClass.computeDestinationPoint(startingPoint, angleFromTrueNorth + 45, this.cellLength*Math.sqrt(2));
                Coordinate destinationPoint3 = HelperClass.computeDestinationPoint(startingPoint, angleFromTrueNorth + 90, this.cellLength);


                ArrayList<Coordinate> cellPolygonCoordinates = new ArrayList<Coordinate>();
                cellPolygonCoordinates.add(startingPoint);
                cellPolygonCoordinates.add(destinationPoint1);
                cellPolygonCoordinates.add(destinationPoint2);
                cellPolygonCoordinates.add(destinationPoint3);
                cellPolygonCoordinates.add(startingPoint);

                // Saving the next cell starting point
                if(j == this.numGridPartitions -1){
                    startingPoint = nextStartingPoint;
                }else{
                    startingPoint = destinationPoint1;
                }

                // Saving the next row starting point
                if(j == 0){
                    nextStartingPoint = destinationPoint3;
                }

                GeometryFactory geofact = new GeometryFactory();
                Polygon cell = geofact.createPolygon(cellPolygonCoordinates.toArray(new Coordinate[0]));
                //System.out.println(cell);

                // Storing the cell into the gridTreeIndex
                gridTreeIndex.insert(cell.getEnvelopeInternal(), cell);
                //cellList.add(cell);

                // Generating and adding the cellKey
                String cellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                girdCellsSet.add(cellKey);
                //System.out.println("cellKey " + cellKey);

                // adding cell and associated key into the map
                cellToKeyMap.put(cell, cellKey);
            }
        }
        //System.out.println("cellToKeyMap Size: " + cellToKeyMap.size());
        //System.out.println(minX + ", " + minY + ", " + maxX + ", " + maxY);

    }

    public String getAngularGridCellKey(org.locationtech.jts.geom.Point p){

        // get candidate cells which may contain the point p
        List<Polygon> candidateCells = gridTreeIndex.query(p.getEnvelopeInternal());

        if(candidateCells.isEmpty()){
            //System.out.println("candidateCells.isEmpty()");
            return null;
        }
        else { // search for the cell(polygon) actually containing the point
            for (Polygon cell : candidateCells) {
                //System.out.println("candidateCells.size() " + candidateCells.size());
                if (cell.covers(p)) {
                    return cellToKeyMap.get(cell);
                }
            }
        }
        return null;

        /*
        double minX = 139.77667562042726;     //X - East-West longitude
		double maxX = 139.77722108273215;
		double minY = 35.6190837;     //Y - North-South latitude
		double maxY = 35.6195177;

		Coordinate startingPoint = new Coordinate(minX, minY);
		double cellLen = 60; // length in meters
        double angle = 45; // in degree

        Coordinate destinationPoint1 = HelperClass.computeDestinationPoint(startingPoint, angle, cellLen);
        Coordinate destinationPoint2 = HelperClass.computeDestinationPoint(startingPoint, angle + 45, cellLen*Math.sqrt(2));
        Coordinate destinationPoint3 = HelperClass.computeDestinationPoint(startingPoint, angle + 90, cellLen);

        // Polygon
        ArrayList<Coordinate> cellPolygonCoordinates = new ArrayList<Coordinate>();
        cellPolygonCoordinates.add(startingPoint);
        cellPolygonCoordinates.add(destinationPoint1);
        cellPolygonCoordinates.add(destinationPoint2);
        cellPolygonCoordinates.add(destinationPoint3);
        cellPolygonCoordinates.add(startingPoint);
//        cellPolygonCoordinates.add(new Coordinate(minX, minY));
//        cellPolygonCoordinates.add(new Coordinate(minX, maxY));
//        cellPolygonCoordinates.add(new Coordinate(maxX, maxY));
//        cellPolygonCoordinates.add(new Coordinate(maxX, minY));
//        cellPolygonCoordinates.add(new Coordinate(minX, minY));

        GeometryFactory geofact = new GeometryFactory();
        Polygon cell = geofact.createPolygon(cellPolygonCoordinates.toArray(new Coordinate[0]));
        //Polygon queryPolygon = new Polygon(queryPolygonCoordinates, uGrid);
         */
    }

    // Getters and Setters
    public STRtree getGridTreeIndex(){
        return gridTreeIndex;
    }

    public boolean getIsAngularGrid(){
        return isAngularGrid;
    }


    public double getMinX() {return minX;}
    public double getMinY() {return minY;}
    public double getMaxX() {return maxX;}
    public double getMaxY() {return maxY;}
    public int getCellIndexStrLength() {return CELLINDEXSTRLENGTH;}

    public int getNumGridPartitions()
    {
        return numGridPartitions;
    }
    public double getCellLength() {return cellLength;}
    public double getCellLengthInMeters() {return cellLengthMeters;}
    public HashSet<String> getGirdCellsSet() {return girdCellsSet;}

    /*
    getGuaranteedNeighboringCells: returns the cells containing the guaranteed r-neighbors
    getCandidateNeighboringCells: returns the cells containing the candidate r-neighbors and require distance computation
    The output set of the above two functions are mutually exclusive
    */
    public HashSet<String> getGuaranteedNeighboringCells(double queryRadius, Point queryPoint)
    {
        //queryRadius = CoordinatesConversion.metersToDD(queryRadius,cellLength,cellLengthMeters); //UNCOMMENT FOR HAVERSINE (METERS)
        //System.out.println("queryRadius in Lat/Lon: "+ queryRadius);
        String queryCellID = queryPoint.gridID;

        HashSet<String> guaranteedNeighboringCellsSet = new HashSet<String>();
        int guaranteedNeighboringLayers = getGuaranteedNeighboringLayers(queryRadius);
        if(guaranteedNeighboringLayers == 0)
        {
            guaranteedNeighboringCellsSet.add(queryCellID);
        }
        else if(guaranteedNeighboringLayers > 0)
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);       //converts cellID String->Integer

            for(int i = queryCellIndices.get(0) - guaranteedNeighboringLayers; i <= queryCellIndices.get(0) + guaranteedNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - guaranteedNeighboringLayers; j <= queryCellIndices.get(1) + guaranteedNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        guaranteedNeighboringCellsSet.add(neighboringCellKey);
                    }
                }
        }

        return guaranteedNeighboringCellsSet;
    }

    public boolean validKey(int x, int y){
        if(x >= 0 && y >= 0 && x < numGridPartitions && y < numGridPartitions)
        {return true;}
        else
        {return false;}
    }

    // Return all the neighboring cells up to the given grid layer
    public HashSet<String> getNeighboringCellsByLayer(Point p, int numNeighboringLayers)
    {
        //queryRadius = CoordinatesConversion.metersToDD(queryRadius,cellLength,cellLengthMeters); // UNCOMMENT FOR HAVERSINE (METERS)
        String givenCellID = p.gridID;
        HashSet<String> neighboringCellsSet = new HashSet<String>();

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(givenCellID);

            for(int i = cellIndices.get(0) - numNeighboringLayers; i <= cellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = cellIndices.get(1) - numNeighboringLayers; j <= cellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }
        }
        return neighboringCellsSet;

    }

    // Return all the neighboring cells including candidate cells and guaranteed cells
    public HashSet<String> getNeighboringCells(double queryRadius, Point queryPoint)
    {
        // return all the cells in the set
        if(queryRadius == 0){
            return this.girdCellsSet;
        }

        //queryRadius = CoordinatesConversion.metersToDD(queryRadius,cellLength,cellLengthMeters); // UNCOMMENT FOR HAVERSINE (METERS)
        String queryCellID = queryPoint.gridID;
        HashSet<String> neighboringCellsSet = new HashSet<String>();
        int numNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);

            for(int i = queryCellIndices.get(0) - numNeighboringLayers; i <= queryCellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - numNeighboringLayers; j <= queryCellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }

        }
        return neighboringCellsSet;
    }

    public HashSet<String> getCandidateNeighboringCells(double queryRadius, Point queryPoint, Set<String> guaranteedNeighboringCellsSet)
    {
        // queryRadius = CoordinatesConversion.metersToDD(queryRadius,cellLength,cellLengthMeters);  //UNCOMMENT FOR HAVERSINE (METERS)
        String queryCellID = queryPoint.gridID;
        HashSet<String> candidateNeighboringCellsSet = new HashSet<String>();
        int candidateNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(candidateNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //candidateNeighboringLayers > 0
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);
            int count = 0;

            for(int i = queryCellIndices.get(0) - candidateNeighboringLayers; i <= queryCellIndices.get(0) + candidateNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - candidateNeighboringLayers; j <= queryCellIndices.get(1) + candidateNeighboringLayers; j++)
                {
                    if(validKey(i,j)) {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        if (!guaranteedNeighboringCellsSet.contains(neighboringCellKey)) // Add key if and only if it exist in the gridCell and is not included in the guaranteed neighbors
                        {
                            count++;
                            candidateNeighboringCellsSet.add(neighboringCellKey);
                        }
                    }
                }
            //System.out.println("Candidate neighbouring cells: " + count);
        }

        return candidateNeighboringCellsSet;
    }

    private int getGuaranteedNeighboringLayers(double queryRadius)
    {

        double cellDiagonal = cellLength*Math.sqrt(2);

        int numberOfLayers = (int)Math.floor((queryRadius/cellDiagonal) - 1); // Subtract 1 because we do not consider the cell with the query object as a layer i
        //System.out.println("Guaranteed Number of Layers: "+ numberOfLayers );
        return numberOfLayers;
        // If return value = -1 then not even the cell containing the query is guaranteed to contain r-neighbors
        // If return value = 0 then only the cell containing the query is guaranteed to contain r-neighbors
        // If return value is a positive integer n, then the n layers around the cell containing the query is guaranteed to contain r-neighbors
    }

    public int getCandidateNeighboringLayers(double queryRadius)
    {
        int numberOfLayers = (int)Math.ceil(queryRadius/cellLength);
        return numberOfLayers;
    }

    public HashSet<String> getNeighboringLayerCells(Point queryPoint, int layerNumber)
    {
        String queryCellID = queryPoint.gridID;
        HashSet<String> neighboringLayerCellsSet = new HashSet<String>();
        HashSet<String> neighboringLayerCellsToExcludeSet = new HashSet<String>();
        ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);       //converts cellID String->Integer

        //Get the cells to exclude, iff layerNumber is greater than 0
        if(layerNumber > 0)
        {
            for(int i = queryCellIndices.get(0) - layerNumber + 1; i <= queryCellIndices.get(0) + layerNumber - 1; i++)
                for(int j = queryCellIndices.get(1) - layerNumber + 1; j <= queryCellIndices.get(1) + layerNumber -1; j++)
                {
                    if(validKey(i,j)) {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringLayerCellsToExcludeSet.add(neighboringCellKey);
                    }
                }
        }

        for(int i = queryCellIndices.get(0) - layerNumber; i <= queryCellIndices.get(0) + layerNumber; i++)
            for(int j = queryCellIndices.get(1) - layerNumber; j <= queryCellIndices.get(1) + layerNumber; j++)
            {
                if(validKey(i,j))
                {
                    String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                    if (!neighboringLayerCellsToExcludeSet.contains(neighboringCellKey)) // Add key if and only if it exist in the gridCell
                    {
                        neighboringLayerCellsSet.add(neighboringCellKey);
                    }
                }
            }
        return neighboringLayerCellsSet;
    }

    // Returns all the neighboring layers of point p, where each layer consists of a number of cells
    public ArrayList<HashSet<String>> getAllNeighboringLayers(Point p)
    {
        ArrayList<HashSet<String>> listOfSets = new ArrayList<HashSet<String>>();

        for(int i = 0; i < numGridPartitions; i++)
        {
            HashSet<String> neighboringLayerCellSet = getNeighboringLayerCells(p, i);

            if(neighboringLayerCellSet.size() > 0)
            {
                listOfSets.add(neighboringLayerCellSet);
            }
            else
            {
                break; // break the for loop as soon as we find an empty neighboringLayerCellSet
            }
        }
        return listOfSets;
    }


    public static class getCellsFilteredByLayer extends RichFilterFunction<Tuple2<String, Integer>>
    {
        private final HashSet<String> CellIDs; // CellIDs are input parameters

        //ctor
        public getCellsFilteredByLayer(HashSet<String> CellIDs)
        {
            this.CellIDs = CellIDs;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> cellIDCount) throws Exception
        {
            return CellIDs.contains(cellIDCount.f0);
        }
    }
}
