package de.farberg.spark.examples.streaming;



import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.io.FileUtils;

import com.google.common.io.Files;
import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.routing.util.EncodingManager;

class GraphhopperHelper {
	private GraphHopper hopper;

	public GraphhopperHelper(File osmFile) throws IOException {
		// create one GraphHopper instance
		this.hopper = new GraphHopper().forServer();
		hopper.setOSMFile(osmFile.toString());

		File tempDirectory = Files.createTempDir();
		FileUtils.forceDeleteOnExit(tempDirectory);
		hopper.setGraphHopperLocation(tempDirectory.toString());
		//hopper.setEncodingManager(new EncodingManager("car"));
		hopper.setEncodingManager(new EncodingManager("foot"));
		hopper.importOrLoad();
	}

	public PathWrapper route(double fromLat, double fromLon, double toLat, double toLon, String weighting, String vehicle) throws Exception {
		GHRequest req = new GHRequest(fromLat, fromLon, toLat, toLon).setWeighting(weighting).setVehicle(vehicle)
				.setLocale(Locale.US);

		GHResponse rsp = hopper.route(req);

		if (rsp.hasErrors()) {
			String errorMessage = "";
			for (Throwable t : rsp.getErrors())
				errorMessage += t.toString();

			throw new Exception(errorMessage);
		}

		return rsp.getBest();
	}
}