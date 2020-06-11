/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package GeoFlink;

import GeoFlink.apps.MFKafkaOutputSchema;
import GeoFlink.apps.MovingFeatures;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.JoinQuery;
import GeoFlink.spatialOperators.KNNQuery;
import GeoFlink.spatialOperators.RangeQuery;
import GeoFlink.spatialStreams.SpatialStream;
import GeoFlink.utils.HelperClass;
import com.typesafe.config.ConfigException;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Serializable;
//import scala.util.parsing.json.JSONObject;
import org.json.*;



import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob implements Serializable {

	public static void main(String[] args) throws Exception {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		//config.setString(RestOptions.BIND_PORT, "8081-8099");  // Can be commented if port is available
		config.setString(RestOptions.BIND_PORT, "8081");

		// set up the streaming execution environment
		final StreamExecutionEnvironment env;
		ParameterTool parameters = ParameterTool.fromArgs(args);

		String inputTopics = parameters.get("inputs");
		String outputTopic = parameters.get("output");
		String queryID = parameters.get("queryId");
		String aggregateFunction = parameters.get("aggregate");  // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
		double cellLengthDesired = Double.parseDouble(parameters.get("cellLength")); // Default 10x10 Grid
		String windowType = parameters.get("wType");  // "TIME" or "COUNT" (Default TIME Window)
		long windowSize = Long.parseLong(parameters.get("wInterval"));
		long windowSlideStep = Long.parseLong(parameters.get("wStep"));
		//String geometryType = parameters.get("gType");
		String geometryCoordinates = parameters.get("gCoordinates");
		//geometryCoordinates = "{ \"Coordinates\": [[[1,2],[13,4],[-1,-6],[7,-8],[6,2]]]}";
		JSONObject inputCoordinatesJSONObj = new JSONObject(geometryCoordinates);
		JSONArray inputCoordinatesArr = inputCoordinatesJSONObj.getJSONArray("coordinates").getJSONArray(0);

		//--inputs "{topics: [MovingFeatures] }" --output "outputTopic" --queryId "Q1" --aggregate "SUM" --cellLength "100.0" --wType "TIME" --wInterval "2" --wStep "1" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		//--inputs "{topics: [MovingFeatures, MovingFeatures2] }" --output "outputTopic3" --queryId "Q1" --aggregate "SUM" --cellLength "10.0" --wType "COUNT" --wInterval "1000"	--wStep "1000" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"

		//queryOption =  Integer.parseInt(parameters.get("queryOption"));
		//radius =  Double.parseDouble(parameters.get("radius"));
			//k = Integer.parseInt(parameters.get("k"));

//			queryOption =  Integer.parseInt(args[1]);
//			radius =  Double.parseDouble(args[2]);
//			uniformGridSize = Integer.parseInt(args[3]);
//			windowSize = Integer.parseInt(args[4]);
//			windowSlideStep = Integer.parseInt(args[5]);
//			k = Integer.parseInt(args[6]);


		env = StreamExecutionEnvironment.getExecutionEnvironment();
		//String bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092";
		String bootStrapServers = "localhost:9092";
		//String topicName = "MovingFeatures";

		// Boundaries for MF datasets
//		double minX = 139.77667562042726;     //X - East-West longitude
//		double maxX = 139.77722108273215;
//		double minY = 35.6190837;     //Y - North-South latitude
//		double maxY = 35.6195177;

		UniformGrid uGrid;
		if (cellLengthDesired > 0) {
			//uGrid = new UniformGrid(cellLengthDesired, minLongitude, maxLongitude, minLatitude, maxLatitude);
			uGrid = new UniformGrid(cellLengthDesired, inputCoordinatesArr);
		}
		else{
			uGrid = new UniformGrid(10, inputCoordinatesArr);
		}

		// Event Time, i.e., the time at which each individual event occurred on its producing device.
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");

		List<DataStream> geoJSONStreams = new ArrayList<DataStream>();
		JSONObject inputTopicsJSONObj = new JSONObject(inputTopics);
		JSONArray inputTopicsArr = inputTopicsJSONObj.getJSONArray("topics");
		for (int i = 0; i < inputTopicsArr.length(); i++)
		{
			String inputTopic = inputTopicsArr.getString(i);
			geoJSONStreams.add(env.addSource(new FlinkKafkaConsumer<>(inputTopic, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()));
			System.out.println(inputTopic);
		}

		List<DataStream<Point>> spatialStreams = new ArrayList<DataStream<Point>>();
		for(DataStream geoJSONStream: geoJSONStreams)
			spatialStreams.add(SpatialStream.PointStream(geoJSONStream, "GeoJSONEventTime", uGrid));

		DataStream<Point> spatialStream = HelperClass.getUnifiedStream(spatialStreams);


		//DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

		// Converting GeoJSON,CSV stream to Spatial data stream (point)
		//DataStream<Point> spatialStream = SpatialStream.PointStream(geoJSONStream, "GeoJSONEventTime", uGrid);
		//DataStream<Point> spatialStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
		//spatialStream.print();

		DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> windowedCellBasedStayTime = MovingFeatures.CellBasedStayTime(spatialStream, aggregateFunction, windowType, windowSize, windowSlideStep);
		windowedCellBasedStayTime.print();

		windowedCellBasedStayTime.addSink(new FlinkKafkaProducer<>(outputTopic, new MFKafkaOutputSchema(outputTopic, queryID, uGrid), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

		// execute program
		env.execute("Geo Flink");
	}
}
