package de.farberg.spark.examples.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.graphhopper.PathWrapper;

import de.farberg.spark.examples.streaming.websiteapplication.LatLon;
import scala.Tuple2;

public class DistanceToClosestTimmy {
	private static List<LatLon> timmys = new ArrayList<>();
	private static GraphhopperHelper helper;
	
	public static class TimmyTravelTime extends LatLon {
		long travelTimeMs;
	}
	static {
		try {
			helper = new GraphhopperHelper(new File("Oshawa-generated.osm.pbf"));
			loadTimmys();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public static Tuple2<Long, Double> travelTimeMsAndDistanceMeter(LatLon from, LatLon dest) throws Exception {
		 PathWrapper route = helper.route(from.lat, from.lon, dest.lat, dest.lon, "fastest", "foot");
		 return new Tuple2<>(route.getTime(), route.getDistance());
	}	
	
	// get timmys locations load it into Array
	private static void loadTimmys() throws FileNotFoundException {
		Scanner inFile = new Scanner(new File("src/main/resources/TLocations.txt"));
		timmys.clear();

		while (inFile.hasNext()) {
			LatLon latlon = new LatLon();
			latlon.lat = Float.valueOf(inFile.next());
			latlon.lon = Float.valueOf(inFile.next());
			timmys.add(latlon);
		}

		inFile.close();
	}

	
	//calculate distance marker and timmys
	public static TimmyTravelTime closestTimmy(double lat, double lon) throws Exception {
		long minTim = Long.MAX_VALUE;
		TimmyTravelTime bestTimmy = null;

		for(LatLon l : timmys) {
			PathWrapper bestPath = helper.route(lat, lon, l.lat, l.lon,	"fastest", "foot");
			if (bestPath.getTime() < minTim) {
				bestTimmy = new TimmyTravelTime();
				bestTimmy.lat = l.lat;
				bestTimmy.lon = l.lon;
				bestTimmy.travelTimeMs = bestPath.getTime();
			}
		}
		
		return bestTimmy;
	}

}
