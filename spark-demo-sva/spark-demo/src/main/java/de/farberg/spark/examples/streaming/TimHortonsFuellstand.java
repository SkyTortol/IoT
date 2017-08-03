package de.farberg.spark.examples.streaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

import de.farberg.spark.examples.streaming.websiteapplication.LatLon;
import scala.Tuple2;

public class TimHortonsFuellstand {
	public static Map<String, Integer> timmys = new HashMap<>();
	public static void main(String[] args) throws NumberFormatException, IOException, FileNotFoundException {
		int maxDistanceToTimmy = 100;

		new Thread(() -> {
			try {
				SumoStaxParser.main(null);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
		
		websiteapplication.main(null);

		// List<LatLng> timHortonsList = Lists.newArrayList(new LatLng(43.9447432,
		// -78.8960708),
		// new LatLng(49.4093582, 8.6947240), new LatLng(49.4895910, 8.4672360));
		//
		BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/TLocations.csv"));
		List<LatLng> timHortonsList = new ArrayList<LatLng>();
		String hline = null;
		while ((hline = reader.readLine()) != null) {
			double lat = Double.parseDouble(hline.split(",")[0]);
			double lon = Double.parseDouble(hline.split(",")[1]);
			System.out.println("Loaded " + lat + "; " + lon + " from csv");
			timHortonsList.add(new LatLng(lat, lon));
		}
		reader.close();
		System.out.println("Loaded " + timHortonsList.size() + " timmys from csv");

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

		JavaRDD<LatLng> timHortons = sc.parallelize(timHortonsList);

		Set<String> topicsSet = new HashSet<>(Arrays.asList("timmys".split(",")));
		Map<String, Integer> topicMap = topicsSet.stream().collect(Collectors.toMap(key -> key, value -> 1));
		// Create a JavaReceiverInputDStream on target ip:port and count the words in
		// input stream of \n delimited text
		JavaPairReceiverInputDStream<String, String> positionOfCarsAsText = KafkaUtils.createStream(ssc,
				"uoit.farberg.de:2181", "timmys", topicMap);
		// String -> LatLng: 49.48893, 8.46726 --> new LatLng(49.48893, 8.46726)
		JavaDStream<LatLng> carPositions = positionOfCarsAsText.map(line -> {
			String[] split = line._2.split(",");
			return new LatLng(Double.parseDouble(split[1]), Double.parseDouble(split[2]));
		});

		// Cartesian Produkt carPositions x timHortons (um eine Distanztabelle
		// aufzubauen)
		JavaDStream<Tuple2<LatLng, LatLng>> distanceMatrix = carPositions.<Tuple2<LatLng, LatLng>>transform(rdd -> {
			JavaPairRDD<LatLng, LatLng> cartesian = rdd.cartesian(timHortons);
			JavaRDD<Tuple2<LatLng, LatLng>> rdd2 = cartesian.rdd().toJavaRDD();
			return rdd2;
		});

		// adding the distance to the coordinates ( Ergänze jeden Eintrag um die
		// Distanz)
		JavaDStream<Tuple2<Tuple2<LatLng, LatLng>, Double>> distances = distanceMatrix.map(coordinates -> {
			double distance = LatLngTool.distance(coordinates._1, coordinates._2, LengthUnit.METER);
			// System.out.println("distance from " + coordinates._1 + " -> " +
			// coordinates._2 + " = " + distance);
			return new Tuple2<Tuple2<LatLng, LatLng>, Double>(coordinates, distance);
		});

		// filter the distance that is too big (Filtere die raus, deren Distanz zu groß
		// ist)
		JavaDStream<Tuple2<Tuple2<LatLng, LatLng>, Double>> closeToTimmy = distances.filter(dist -> {
			// if (dist._2.doubleValue() < maxDistanceToTimmy) System.err.println("Keeping
			// timmy " + dist._1._1 + " / " + dist._1._2);
			return dist._2.doubleValue() < maxDistanceToTimmy;
		});

		// Map Timmy -> 1
		JavaPairDStream<String, Integer> timmyTo1 = closeToTimmy.mapToPair(dist -> {
			//System.out.println("Timmy " + dist._1._2.toString());
			return new Tuple2<String, Integer>(dist._1._2.toString(), 1);
		});

		// Count per Key
		JavaPairDStream<String, Integer> countPerTimmy = timmyTo1.reduceByKey((x, y) -> x + y);
		countPerTimmy.count().print();
		countPerTimmy.print(20);
		

		countPerTimmy.foreachRDD(rdd -> {
			rdd.foreach(tuple -> {
				timmys.put(tuple._1, tuple._2);
			});
		});
		
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		sc.close();

	}

}