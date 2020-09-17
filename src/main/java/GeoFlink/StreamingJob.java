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
import org.apache.flink.api.common.functions.CoGroupFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
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



		String queryOption = parameters.get("queryOption");
		String inputTopics = parameters.get("inputs");
		String outputTopic = parameters.get("output");
		String queryID = parameters.get("queryId");
		String aggregateFunction = parameters.get("aggregate");  // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
		double cellLengthDesired = Double.parseDouble(parameters.get("cellLength")); // Default 10x10 Grid
		double movingSensorRadius = Double.parseDouble(parameters.get("sensorRadius"));

		double gridAngle = Double.parseDouble(parameters.get("gAngle"));
		int gRows = Integer.parseInt(parameters.get("gRows"));
		int gColumns = Integer.parseInt(parameters.get("gColumns"));

		//int operatorParallelism = Integer.parseInt(parameters.get("parallelism"));

		String windowType = parameters.get("wType");  // "TIME" or "COUNT" (Default TIME Window)
		long windowSize = Long.parseLong(parameters.get("wInterval"));
		long windowSlideStep = Long.parseLong(parameters.get("wStep"));
		//String geometryType = parameters.get("gType");
		String geometryCoordinates = parameters.get("gCoordinates");
		//geometryCoordinates = "{ \"Coordinates\": [[[1,2],[13,4],[-1,-6],[7,-8],[6,2]]]}";
		JSONObject inputCoordinatesJSONObj = new JSONObject(geometryCoordinates);
		JSONArray inputCoordinatesArr = inputCoordinatesJSONObj.getJSONArray("coordinates").getJSONArray(0);


		//"--gPointCoordinates", "{coordinates: [139.77667562042726, 35.6190837]"
		String gridPoint = parameters.get("gPointCoordinates");
		//System.out.println("gridPoint :" + gridPoint);
		JSONObject gridPointCoordinatesJSONObj = new JSONObject(gridPoint);
		JSONArray gridPointCoordinatesArr = gridPointCoordinatesJSONObj.getJSONArray("coordinates");

		//--inputs "{topics: [MovingFeatures] }" --output "outputTopic" --queryId "Q1" --aggregate "SUM" --cellLength "100.0" --wType "TIME" --wInterval "2" --wStep "1" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		//--inputs "{topics: [MovingFeatures, MovingFeatures2] }" --output "outputTopic3" --queryId "Q1" --aggregate "SUM" --cellLength "10.0" --wType "COUNT" --wInterval "1000"	--wStep "1000" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		//--inputs "{movingObjTopics: [MovingFeatures, MovingFeatures2], sensorTopics: [MovingFeatures2]}" --output "outputTopic3" --queryId "Q1" --sensorRadius "0.001" --aggregate "SUM" --cellLength "10.0" --wType "COUNT" --wInterval "1000" --wStep "1000" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		//--queryOption "stayTime" --inputs "{movingObjTopics: [MovingFeatures, MovingFeatures2], sensorTopics: [MovingFeatures2]}" --output "outputTopic" --queryId "Q1" --sensorRadius "0.0005" --aggregate "AVG" --cellLength "10.0" --wType "TIME" --wInterval "10" --wStep "10" --gType "Polygon" --gCoordinates "{coordinates: [[[139.7766, 35.6190], [139.7766, 35.6196], [139.7773, 35.6190],[139.7773, 35.6196], [139.7766, 35.6196]]]}"
		//--inputs "{topics: [MovingFeatures] }" --output "outputTopic" --queryId "Q1" --aggregate "SUM" --cellLength "100.0" --wType "TIME" --wInterval "2" --wStep "1" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		//--inputs "{topics: [MovingFeatures, MovingFeatures2] }" --output "outputTopic3" --queryId "Q1" --aggregate "SUM" --cellLength "10.0" --wType "COUNT" --wInterval "1000"	--wStep "1000" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		//--inputs "{movingObjTopics: [MovingFeatures, MovingFeatures2], sensorTopics: [MovingFeatures2]}" --output "outputTopic3" --queryId "Q1" --sensorRadius "0.001" --aggregate "SUM" --cellLength "10.0" --wType "COUNT" --wInterval "1000" --wStep "1000" --gType "Polygon" --gCoordinates "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}"
		// --queryOption "stayTimeAngularGrid" --inputs "{movingObjTopics: [MovingFeatures, MovingFeatures2], sensorTopics: [MovingFeatures2]}" --output "outputTopic" --queryId "Q1" --sensorRadius "0.0005" --aggregate "AVG" --cellLength "2" --wType "TIME" --wInterval "10" --wStep "10" --gType "Polygon" --gCoordinates "{coordinates: [[[139.7766, 35.6190], [139.7766, 35.6196], [139.7773, 35.6190],[139.7773, 35.6196], [139.7766, 35.6196]]]}" --gPointCoordinates "{coordinates: [139.77667562042726, 35.6190837]}" --gPointCoordinates2 "{coordinates: [139.7766, 35.6190]}" --gRows 35 --gColumns 20 --gAngle 45
		// --queryOption "stayTimeAngularGrid" --inputs "{movingObjTopics: [TaxiDrive17MillionGeoJSON], sensorTopics: [MovingFeatures2]}" --output "outputTopic" --queryId "Q1" --sensorRadius "0.0005" --aggregate "AVG" --cellLength "2" --wType "TIME" --wInterval "10" --wStep "10" --gType "Polygon" --gCoordinates "{coordinates: [[[115.50000, 39.60000], [115.50000, 41.10000], [117.60000, 41.10000],[117.60000, 39.60000], [115.50000, 39.60000]]]}" --gPointCoordinates "{coordinates: [115.50000, 39.60000]}" --gRows 1000 --gColumns 20 --gAngle 45

		// For GUI execution
		//--queryOption="stayTimeAngularGrid" --inputs="{movingObjTopics: [TaxiDrive17MillionGeoJSON]}" --output="outputTopic" --queryId="Q1" --sensorRadius="0.0005" --aggregate="AVG" --cellLength="360" --wType="TIME" --wInterval="10" --wStep="10" --gType="Polygon" --gCoordinates="{coordinates: [[[115.50000, 39.60000], [115.50000, 41.10000], [117.60000, 41.10000],[117.60000, 39.60000], [115.50000, 39.60000]]]}" --gPointCoordinates="{coordinates: [115.50000, 39.60000]}" --gRows="50" --gColumns="20" --gAngle="45"

		// Cluster
		//env = StreamExecutionEnvironment.getExecutionEnvironment();
		//String bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092";

		// Local
		env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		String bootStrapServers = "localhost:9092";

		// Event Time, i.e., the time at which each individual event occurred on its producing device.
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.setParallelism(30);
		//env.setParallelism(operatorParallelism);

		// Boundaries for MF datasets
//		double minX = 139.77667562042726;     //X - East-West longitude
//		double maxX = 139.77722108273215;
//		double minY = 35.6190837;     //Y - North-South latitude
//		double maxY = 35.6195177;

		UniformGrid uGrid;

		// Angular UGrid
		if (cellLengthDesired > 0 && gridAngle != 0 && (gRows > 0 || gColumns > 0)){  // gridAngle removed for the sake of demo only
			uGrid = new UniformGrid(gridPointCoordinatesArr, gridAngle, cellLengthDesired, gRows, gColumns);
		}
		else if (cellLengthDesired > 0 && gridAngle == 0 && (gRows > 0 || gColumns > 0)){  // Ordinary UGrid
			uGrid = new UniformGrid(gridPointCoordinatesArr, cellLengthDesired, gRows, gColumns);
		}
		else if (cellLengthDesired > 0) { // Ordinary UGrid
			uGrid = new UniformGrid(cellLengthDesired, inputCoordinatesArr);
		}
		else{
			uGrid = new UniformGrid(10, inputCoordinatesArr);
		}


		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");


		List<DataStream> geoJSONStreams = new ArrayList<DataStream>();
		JSONObject inputTopicsJSONObj = new JSONObject(inputTopics);

		// Ordinary Stream
		JSONArray movingObjTopicsArr = inputTopicsJSONObj.getJSONArray("movingObjTopics");
		for (int i = 0; i < movingObjTopicsArr.length(); i++)
		{
			String inputTopic = movingObjTopicsArr.getString(i);
			geoJSONStreams.add(env.addSource(new FlinkKafkaConsumer<>(inputTopic, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()));
		}
		List<DataStream<Point>> spatialStreams = new ArrayList<DataStream<Point>>();
		for(DataStream geoJSONStream: geoJSONStreams)
			spatialStreams.add(SpatialStream.PointStream(geoJSONStream, "GeoJSONEventTime", uGrid));
		DataStream<Point> spatialStream = HelperClass.getUnifiedStream(spatialStreams);
		//spatialStream.print();

		/*
		// Sensor Stream
		geoJSONStreams.clear();
		JSONArray sensorTopicsArr = inputTopicsJSONObj.getJSONArray("sensorTopics");
		for (int i = 0; i < sensorTopicsArr.length(); i++)
		{
			String inputTopic = sensorTopicsArr.getString(i);
			geoJSONStreams.add(env.addSource(new FlinkKafkaConsumer<>(inputTopic, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()));
		}
		List<DataStream<Point>> sensorSpatialStreams = new ArrayList<DataStream<Point>>();
		for(DataStream geoJSONStream: geoJSONStreams)
			sensorSpatialStreams.add(SpatialStream.PointStream(geoJSONStream, "GeoJSONEventTime", uGrid));
		DataStream<Point> sensorSpatialStream = HelperClass.getUnifiedStream(sensorSpatialStreams);
		 */



		// Switch to select a specified query
		switch(queryOption) {
			case "stayTime": {
				// Moving Objects Stay Time
				DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> windowedCellBasedStayTime = MovingFeatures.CellBasedStayTime(spatialStream, aggregateFunction, windowType, windowSize, windowSlideStep);
				//windowedCellBasedStayTime.print();
				windowedCellBasedStayTime.addSink(new FlinkKafkaProducer<>(outputTopic, new MFKafkaOutputSchema(outputTopic, queryID, aggregateFunction, uGrid), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case "stayTimeAngularGrid": {

				DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> windowedCellBasedStayTimeAngularGrid;
				// Moving Objects Stay Time
				if (gridAngle != 0) {
					windowedCellBasedStayTimeAngularGrid = MovingFeatures.CellBasedStayTimeAngularGrid(spatialStream, aggregateFunction, windowType, windowSize, windowSlideStep);
				}
				else{ // No need to use angular grid if the gridAngle = 0
					windowedCellBasedStayTimeAngularGrid = MovingFeatures.CellBasedStayTime(spatialStream, aggregateFunction, windowType, windowSize, windowSlideStep);
				}
				windowedCellBasedStayTimeAngularGrid.print();
				windowedCellBasedStayTimeAngularGrid.addSink(new FlinkKafkaProducer<>(outputTopic, new MFKafkaOutputSchema(outputTopic, queryID, aggregateFunction, uGrid), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case "stayTimeWEmptyCells": {
				// stayTime Query With Empty Cells
				//DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> stayTimeWEmptyCells = MovingFeatures.CellBasedStayTimeWEmptyCells(spatialStream, sensorSpatialStream, movingSensorRadius, aggregateFunction, windowSize, windowSlideStep, uGrid);
				//stayTimeWEmptyCells.print();
				//stayTimeWEmptyCells.addSink(new FlinkKafkaProducer<>(outputTopic, new MFKafkaOutputSchema(outputTopic, queryID, aggregateFunction, uGrid), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			default:
				System.out.println("Input Unrecognized. queryOption must be stayTime OR stayTimeWEmptyCells.");
		}

		// execute program
		env.execute("Geo Flink");
	}
}