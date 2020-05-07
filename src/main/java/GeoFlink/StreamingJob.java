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

import GeoFlink.apps.MovingFeatures;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.JoinQuery;
import GeoFlink.spatialOperators.KNNQuery;
import GeoFlink.spatialOperators.RangeQuery;
import GeoFlink.spatialStreams.SpatialStream;
import GeoFlink.utils.HelperClass;
import com.typesafe.config.ConfigException;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import scala.Serializable;


import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;

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
		config.setString(RestOptions.BIND_PORT, "8081-8099");  // Can be commented if port is available

		// set up the streaming execution environment
		final StreamExecutionEnvironment env;

		//INPUT FORMAT FOR CLUSTER: <True> <Query_number> <Radius> <Grid_size> <Window_size> <Slide_step>
		if(args.length < 1)
		{
			System.out.println("At-leaset one argument must be provided. |true| for cluster and |false| for local processing");
			System.exit(0);
		}

		boolean onCluster = Boolean.parseBoolean(args[0]);
		String topicName;
		String bootStrapServers;
		int queryOption = 1;
		double radius = 0;
		int uniformGridSize = 100;
		int windowSlideStep = 0;
		int windowSize = 0;
		int k = 3; // default value

		if (onCluster) {

			if(args.length < 7)
			{
				System.out.println("Input argument if onCluster (true/false) and the query option.");
				System.out.println("INPUT FORMAT FOR CLUSTER: <True> <Query_number> <Radius> <Grid_size> <Window_size> <Slide_step> <k>, E.g.: True 1 0.01 100 1 1 3");
				System.exit(0);
			}

			env = StreamExecutionEnvironment.getExecutionEnvironment();

			queryOption =  Integer.parseInt(args[1]);
			radius =  Double.parseDouble(args[2]);
			uniformGridSize = Integer.parseInt(args[3]);
			windowSize = Integer.parseInt(args[4]);
			windowSlideStep = Integer.parseInt(args[5]);
			k = Integer.parseInt(args[6]);
			bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092";
			topicName = "TaxiDriveGeoJSON_17M_R2_P60";

		}else{

			System.out.println("Local Execution!");
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

			queryOption = 1; //1,2 Range Query, 4, 5 kNN Query, 7, 8 Spatial Join
			radius =  0.004;
			uniformGridSize = 100;
			windowSize = 1;
			windowSlideStep = 1;
			k = 3;
			bootStrapServers = "localhost:9092";
			//topicName = "TaxiDrive17MillionGeoJSON";
			topicName = "MovingFeatures";
		}

		// Defining Grid

		// Boundaries for Foursquare check-in datasets
		//double maxX = -73.683825;       //X - East-West longitude
		//double minX = -74.274766;
		//double maxY = 40.988332;        //Y - North-South latitude
		//double minY = 40.550852;

		// Boundaries for Taxi Drive datasets
		//double minX = 115.50000;     //X - East-West longitude
		//double maxX = 117.60000;
		//double minY = 39.60000;     //Y - North-South latitude
		//double maxY = 41.10000;

		// Boundaries for MF datasets
		double minX = 139.77667562042726;     //X - East-West longitude
		double maxX = 139.77722108273215;
		double minY = 35.6190837;     //Y - North-South latitude
		double maxY = 35.6195177;

		UniformGrid uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

		// Event Time, i.e., the time at which each individual event occurred on its producing device.
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");
		DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

		// Converting GeoJSON,CSV stream to Spatial data stream (point)
		//DataStream<Point> spatialStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
		DataStream<Point> spatialStream = SpatialStream.PointStream(geoJSONStream, "GeoJSONEventTime", uGrid);
		//DataStream<Point> spatialStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);



		DataStream<Tuple5<String, Integer, Long, Long, HashMap<Integer, Long>>> windowedCellBasedStayTime = MovingFeatures.CellBasedStayTime(spatialStream, windowSize, windowSlideStep);
		windowedCellBasedStayTime.print();

		/*
		Point queryPoint = new Point(116.414899, 39.920374, uGrid);
		switch(queryOption) {

			case 1: { // Range Query (Grid-based)
				DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
				break;}
			case 2: { // KNN (Grid based - Iterative approach)
				DataStream < PriorityQueue < Tuple2 < Point, Double >>> kNNPQStream = KNNQuery.SpatialKNNQuery(spatialStream, queryPoint, k, windowSize, windowSlideStep, uGrid);
				break;}
			case 3: { // Spatial Join (Grid-based)
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDriveQueries1MillionGeoJSON_Live", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				//DataStream<Point> queryStream = queryStreamJSON.map(new SpatialStream.GeoJSONToSpatial(uGrid)).startNewChain();
				DataStream<Point> queryStream = SpatialStream.PointStream(geoJSONQueryStream, "GeoJSON", uGrid);
				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialStream, queryStream, radius, windowSize, windowSlideStep, uGrid);
				break;}
			default:
				System.out.println("Input Unrecognized. Please select option from 1-3.");
		}
		*/

		// execute program
		env.execute("Geo Flink");
	}
}
