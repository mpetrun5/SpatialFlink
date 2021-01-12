package GeoFlink.apps;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
//import jdk.nashorn.api.scripting.JSObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.*;


public class MFKafkaOutputSchema implements Serializable, KafkaSerializationSchema<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> {

    private String outputTopic;
    private String queryId;
    private UniformGrid uGrid;
    private String aggregateFunction;

    public MFKafkaOutputSchema(String outputTopicName, String queryID, String aggregateFunction, UniformGrid uGrid)
    {
        this.outputTopic = outputTopicName;
        this.queryId = queryID;
        this.aggregateFunction = aggregateFunction;
        this.uGrid = uGrid;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> element, @Nullable Long timestamp) {

        ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(element.f0);
        List<Tuple2<Double, Double>> cellCoordinates;

        if(this.uGrid.getIsAngularGrid()){
            cellCoordinates = uGrid.getAngularGridCellCoordinates(element.f0);
        }
        else{
            cellCoordinates = HelperClass.getCellCoordinates(cellIndices, this.uGrid);
        }

        JSONObject jsonObj = new JSONObject();

        //JSONObject outputObj = new JSONObject();
        //jsonObj.put("output", outputObj);

        JSONObject propertiesObj = new JSONObject();

        jsonObj.put("properties", propertiesObj);
        propertiesObj.put("aggregate", this.aggregateFunction);

        JSONArray window = new JSONArray();
        window.put(element.f2);
        window.put(element.f3);
        propertiesObj.put("window", window);

        JSONArray cellIds = new JSONArray();
        cellIds.put(cellIndices.get(0));
        cellIds.put(cellIndices.get(1));
        propertiesObj.put("cellIndices", cellIds);

        jsonObj.put("type", "Feature");

        JSONObject geometryObj = new JSONObject();
        jsonObj.put("geometry", geometryObj);

        geometryObj.put("type", "Polygon");

        JSONArray coordinates = new JSONArray();
        JSONArray innerCoordinatesArray = new JSONArray();
        coordinates.put(innerCoordinatesArray);

        geometryObj.put("coordinates", coordinates);
        //POLYGON ((139.77670226414034 35.619087819229755, 139.77670227379417 35.61908826882146, 139.7767028268609 35.6190882609738, 139.77670281720708 35.6190878113821, 139.77670226414034 35.619087819229755))

        //String poly = "POLYGON ((";
        for(Tuple2<Double, Double> cellCoordinate: cellCoordinates) {
            JSONArray coordinatePoint = new JSONArray();
            coordinatePoint.put(cellCoordinate.f0);
            coordinatePoint.put(cellCoordinate.f1);
            innerCoordinatesArray.put(coordinatePoint);
            //poly += cellCoordinate.f0 + " " + cellCoordinate.f1 + ",";
        }

        //poly = poly.substring(0, poly.length()-1);
        //poly += "))";
        //System.out.println(poly);
        propertiesObj.put("numOfObjects", element.f1);
        propertiesObj.put("queryId", this.queryId);

        JSONArray stayTime = new JSONArray();
        propertiesObj.put("stayTime", stayTime);

        for (Map.Entry<Integer, Long> entry : element.f4.entrySet()){
            JSONObject stayTimeObj = new JSONObject();
            stayTimeObj.put(entry.getKey().toString(), entry.getValue());
            stayTime.put(stayTimeObj);
        }

        return new ProducerRecord<byte[], byte[]>(outputTopic, jsonObj.toString().getBytes(StandardCharsets.UTF_8));
    }


}
