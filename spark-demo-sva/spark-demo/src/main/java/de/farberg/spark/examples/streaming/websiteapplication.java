package de.farberg.spark.examples.streaming;

import static spark.Spark.staticFiles;

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.gson.Gson;

import scala.Tuple2;
import spark.Spark;

public class websiteapplication {

	public static class LatLon {
		double lat;
		double lon;

	}

	public static class Response {
		LatLon closestTimmy;
		double waitingTimeMs;
		String info;
	}

	public static void main(String[] args) {
		staticFiles.location("/public"); // call website
		Pattern pattern = Pattern.compile("\\((.+),(.+)\\)"); // Lat, lon from String into a pattern

		// get coordinates from leaflet
		Spark.post("/coordinates", (req, res) -> {
			String debug = "";
			Gson gson = new Gson();
			LatLon fromWebbrowser = gson.fromJson(req.body(), LatLon.class);

			AtomicReference<LatLon> closestTimmy = new AtomicReference<>();
			AtomicDouble shortestTime = new AtomicDouble(Double.MAX_VALUE);

			debug += "Received " + fromWebbrowser.lat + ", " + fromWebbrowser.lon + "\n";

			// calculating waiting minutes from people count in timmys
			for (Entry<String, Integer> tuple : TimHortonsFuellstand.timmys.entrySet()) {
				int waitingPeople = tuple.getValue();
				double waitingTimeMs = waitingPeople * 60 * 1000;

				debug += "Looking at " + tuple.getKey() + ", " + tuple.getValue() + " cars close to timmy" + "\n";

				// calculating closest distance to timmy and waiting time -> best timmys
				Matcher matcher = pattern.matcher(tuple.getKey());
				if (matcher.matches()) {
					LatLon timmy = new LatLon();
					timmy.lat = Double.parseDouble(matcher.group(1));
					timmy.lon = Double.parseDouble(matcher.group(2));

					Tuple2<Long, Double> travelTimeMsAndDistanceMeter = DistanceToClosestTimmy
							.travelTimeMsAndDistanceMeter(fromWebbrowser, timmy);
					double travelTimeMs = travelTimeMsAndDistanceMeter._1;
					double distanceMeter = travelTimeMsAndDistanceMeter._2;
					double overallWaitingTime = waitingTimeMs + travelTimeMs;

					debug += "distance from " + fromWebbrowser.lat + "," + fromWebbrowser.lon + " to " + timmy.lat + ","
							+ timmy.lon + " = " + distanceMeter + "m\n";
					debug += "travelTimeMs(" + travelTimeMs + ") + waitingTimeMs(" + waitingTimeMs
							+ ") = overallWaitingTime: " + overallWaitingTime + "ms" + "\n";

					if (shortestTime.get() > overallWaitingTime) {
						closestTimmy.set(timmy);
						shortestTime.set(overallWaitingTime);
						debug += "New best timmy at " + closestTimmy.get().lat + "," + closestTimmy.get().lon + ", " + shortestTime.get() + "ms" + "\n";

					}

				}
			}

			Response response = new Response();
			response.closestTimmy = closestTimmy.get();
			double waitingTimeConverted = shortestTime.get() / 60000.0f ;
			double waitingTimeShort = Math.round(waitingTimeConverted*100)/100;
			response.waitingTimeMs = waitingTimeShort;
			response.info = debug;
			System.out.println(response.closestTimmy);
			System.out.println(debug);
			System.out.println(response);

			res.type("application/json");
			return gson.toJson(response);
		});
	}

}