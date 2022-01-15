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

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.JoinQuery;
import GeoFlink.spatialOperators.KNNQuery;
import GeoFlink.spatialStreams.Deserialization;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import scala.Serializable;

import java.util.PriorityQueue;
import java.util.Properties;

public class JoinQueryExample implements Serializable {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env;
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(30);

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", "localhost:9094");

		double minX = 115.50000;
		double maxX = 117.60000;
		double minY = 39.60000;
		double maxY = 41.10000;
		double uniformGridSize = 100;

		// Defining Grid
		UniformGrid uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

		// Generating stream
		DataStream<String> inputStream  = env.addSource(new FlinkKafkaConsumer("GeoLife", new JSONKeyValueDeserializationSchema(false), kafkaProperties));
		DataStream<Point> originalStream = Deserialization.PointStream(inputStream, "GeoJSON", uGrid);
		DataStream<String> inputQueryStream  = env.addSource(new FlinkKafkaConsumer("QueryData", new JSONKeyValueDeserializationSchema(false), kafkaProperties));
		DataStream<Point> queryStream = Deserialization.PointStream(inputQueryStream, "GeoJSON", uGrid);

		double queryRadius = 1;

		DataStream outputStream = JoinQuery.PointJoinQuery(originalStream, queryStream, queryRadius, 10, uGrid, uGrid, 5, true);
		outputStream.print();

		env.execute("GeoFlink");
	}


}
