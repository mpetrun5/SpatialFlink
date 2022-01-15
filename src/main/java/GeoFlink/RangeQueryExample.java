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

import GeoFlink.apps.StayTime;
import GeoFlink.apps.CheckIn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.*;
import GeoFlink.spatialOperators.*;
import GeoFlink.spatialStreams.*;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.shaded.guava18.com.google.common.collect.Multimap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.impl.CsvDecoder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.locationtech.jts.geom.Coordinate;
import scala.Serializable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RangeQueryExample implements Serializable {

	public static void main(String[] args) throws Exception {
		// Set up the streaming execution environment
		final StreamExecutionEnvironment env;

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(30);

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", "localhost:9094");
		// kafkaProperties.setProperty("group.id", "messageStream");

		double minX = 115.50000;
		double maxX = 117.60000;
		double minY = 39.60000;
		double maxY = 41.10000;
		double uniformGridSize = 100;

		// Defining Grid
		UniformGrid uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);
		// qPoint = new Point(116.14319183444924, 40.07271444145411, uGrid);

		// Generating stream
		DataStream<String> inputStream  = env.addSource(new FlinkKafkaConsumer("GeoLife", new JSONKeyValueDeserializationSchema(false), kafkaProperties));
		DataStream<Point> spatialPointStream = Deserialization.PointStream(inputStream, "GeoJSON", uGrid);


		Point qPoint = new Point(116.14319183444924, 40.07271444145411, uGrid);
		double queryRadius = 0.5;

		DataStream<Point> neighbors = RangeQuery.PointRangeQuery(spatialPointStream, qPoint, queryRadius, uGrid, true);
		neighbors.print();

		env.execute("GeoFlink");
	}


}
