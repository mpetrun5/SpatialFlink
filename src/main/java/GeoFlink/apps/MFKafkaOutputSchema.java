package GeoFlink.apps;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MFKafkaOutputSchema implements Serializable, KafkaSerializationSchema<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> {

    private String outputTopic;

    public MFKafkaOutputSchema(String outputTopicName)
    {
        this.outputTopic = outputTopicName;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>> element, @Nullable Long timestamp) {

        String jsonStr = "{\"cellID\": \"" + element.f0 + "\", \"numCellObjects\": " + element.f1 + ", \"winStart\": " + element.f2 + ", \"winEnd\": " + element.f3 +  ", \"objects\": [ ";

        for (Map.Entry<Integer, Long> entry : element.f4.entrySet()){
            jsonStr += "[\"" + entry.getKey() + "\" , " + entry.getValue() + "],";
        }

        // Removing the last comma
        if (jsonStr.length() > 0 && jsonStr.charAt(jsonStr.length() - 1) == ',') {
            jsonStr = jsonStr.substring(0, jsonStr.length() - 1);
        }

        jsonStr += "]}";

        return new ProducerRecord<byte[], byte[]>(outputTopic, jsonStr.getBytes(StandardCharsets.UTF_8));
    }
}
