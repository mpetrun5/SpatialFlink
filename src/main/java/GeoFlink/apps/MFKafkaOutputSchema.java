package GeoFlink.apps;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import jdk.nashorn.api.scripting.JSObject;
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

    public MFKafkaOutputSchema(String outputTopicName, String queryID, UniformGrid uGrid)
    {
        this.outputTopic = outputTopicName;
        this.queryId = queryID;
        this.uGrid = uGrid;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> element, @Nullable Long timestamp) {

        List<Tuple2<Double, Double>> cellCoordinates = HelperClass.getCellCoordinates(element.f0, this.uGrid);

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("queryId", this.queryId);

        JSONObject outputObj = new JSONObject();
        jsonObj.put("output", outputObj);

        JSONArray window = new JSONArray();
        window.put(element.f2);
        window.put(element.f3);

        outputObj.put("window", window);
        outputObj.put("cellId", element.f0);

        JSONArray coordinates = new JSONArray();
        outputObj.put("coordinates", coordinates);

        for(Tuple2<Double, Double> cellCoordinate: cellCoordinates) {
            JSONArray coordinatePoint = new JSONArray();
            coordinatePoint.put(cellCoordinate.f0);
            coordinatePoint.put(cellCoordinate.f1);
            coordinates.put(coordinatePoint);
        }

        outputObj.put("numOfObjects", element.f1);

        JSONArray stayTime = new JSONArray();
        outputObj.put("stayTime", stayTime);

        for (Map.Entry<Integer, Long> entry : element.f4.entrySet()){
            JSONObject stayTimeObj = new JSONObject();
            stayTimeObj.put(entry.getKey().toString(), entry.getValue());
            stayTime.put(stayTimeObj);
        }

        return new ProducerRecord<byte[], byte[]>(outputTopic, jsonObj.toString().getBytes(StandardCharsets.UTF_8));
    }


}
