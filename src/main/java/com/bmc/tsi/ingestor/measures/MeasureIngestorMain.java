package com.bmc.tsi.ingestor.measures;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.poi.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class MeasureIngestorMain {

	private static final Logger LOGGER = LoggerFactory.getLogger(MeasureIngestorMain.class);
	private static final String NOINSTANCE = "";
	private static TreeMap<String, List<String>> sourceAppMapping = new TreeMap<>();
	private static TreeMap<String, TreeMap<String, TreeMap<String, TreeSet<Measure>>>> sourceMetricMap = new TreeMap<>();

	public static void main(String args[]) throws Exception {
		System.out.println("MB: "
				+ (double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
		long start = System.currentTimeMillis();

		String dataFilePath = args[0];
		String masterFilePath = args[1];

		MeasureIngestorMain main = new MeasureIngestorMain();
		main.loaddataFile(dataFilePath);
		main.loadMaster(masterFilePath);
		long end = System.currentTimeMillis();
		System.out.println("MB: "
				+ (double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024)
				+ "  Time(ms) " + (end - start));

		main.writeBackMaster();
		main.writeBackData();
	}

	private void loadMaster(String fileName) {
		Stream<String> fileStream = null;
		try {
			fileStream = Files.lines(Paths.get(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		fileStream.forEach(this::masterAction);
	}

	private void masterAction(String line) {
		String[] tokens = line.split(",");
		List<String> appList = new ArrayList<>();

		for (int i = 1; i < tokens.length; i++) {
			// for lines like lonindapnp262.uk.db.com,"Transaction
			// Warehouse,db-Unity"
			// where we have more than one App, mapped to a source
			tokens[i].replace("\"", "");
			appList.add(tokens[i]);
		}
		sourceAppMapping.put(tokens[0], appList);
	}

	private void loaddataFile(String fileName) {
		Stream<String> fileStream = null;
		try {
			fileStream = Files.lines(Paths.get(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		fileStream.skip(1).forEach(this::dataAction);
	}

	private void dataAction(String line) {
		String[] tokens = line.split(",");
		String sourceName, instanceName, metricName;
		sourceName = tokens[0];

		// For rows like
		// londhso2d7b.uk.db.com;/usr/local,diskusage,34,1502035500
		// where the disk volume if also present

		if (tokens[0].contains(";")) {
			String[] sourceSplit = tokens[0].split(";");
			sourceName = sourceSplit[0];
			instanceName = sourceSplit[1];
		} else {
			sourceName = tokens[0];
			instanceName = NOINSTANCE;

		}

		metricName = tokens[1];

		TreeMap<String, TreeMap<String, TreeSet<Measure>>> instanceMap = sourceMetricMap.getOrDefault(sourceName,
				new TreeMap<String, TreeMap<String, TreeSet<Measure>>>());

		TreeMap<String, TreeSet<Measure>> metricMap = instanceMap.getOrDefault(instanceName,
				new TreeMap<String, TreeSet<Measure>>());

		TreeSet<Measure> measureSet = metricMap.getOrDefault(metricName, new TreeSet<Measure>());
		try {
			float value = 0f;
			try {
				value = Float.parseFloat(tokens[2]);
			} catch (NumberFormatException nfe) {
				LOGGER.debug("Got NumberFormatException for value {} in line {}", tokens[2], line);
			}

			measureSet.add(new Measure(value, Long.parseLong(tokens[3])));
			metricMap.put(metricName, measureSet);
			instanceMap.put(instanceName, metricMap);
			sourceMetricMap.put(sourceName, instanceMap);
		} catch (NumberFormatException nfe) {

		}
	}

	private void writeBackMaster() {

		sourceAppMapping.forEach(this::writeBackMaster);
	}

	private void writeBackMaster(String source, List<String> apps) {
		try {
			Files.write(Paths.get("C:\\data\\DBDataIngestion\\serverAppReal_new"),
					(source + "," + StringUtil.join(apps.toArray(new String[apps.size()]), ",") + "\n").getBytes(),
					StandardOpenOption.APPEND, StandardOpenOption.CREATE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void writeBackData() {

		sourceMetricMap.forEach(this::writeBackData);
	}

	private void writeBackData(String source, TreeMap<String, TreeMap<String, TreeSet<Measure>>> instanceMap) {
		try {
			for (String instanceEntry : instanceMap.keySet()) {
				Map<String, TreeSet<Measure>> metricInstanceMap = instanceMap.get(instanceEntry);
				for (String metricName : metricInstanceMap.keySet()) {
					for (Measure measure : metricInstanceMap.get(metricName)) {
						String timestamp = String.valueOf(measure.getTimestamp());
						String value = (measure.getValue() == 0f) ? "" : measure.getValue() + "";
						StringJoiner line = new StringJoiner(",");
						if (!instanceEntry.equals(NOINSTANCE)) {
							line.add(source + ";" + instanceEntry);
						} else {
							line.add(source);
						}
						line.add(metricName).add(value).add(timestamp);
						Files.write(Paths.get("C:\\data\\DBDataIngestion\\data_new"),
								(line.toString() + "\n").getBytes(), StandardOpenOption.APPEND,
								StandardOpenOption.CREATE);
					}
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public class SendTask implements Runnable {

		String sourceID;
		String appID;

		public SendTask(String sourceID, String appID) {
			this.sourceID = sourceID;
			this.appID = appID;
		}

		@Override
		public void run() {

			Map<String, TreeMap<String, TreeSet<Measure>>> instanceMap = sourceMetricMap.get(sourceID);

			for (String instanceEntry : instanceMap.keySet()) {
				Map<String, TreeSet<Measure>> metricInstanceMap = instanceMap.get(instanceEntry);
				for (String metricName : metricInstanceMap.keySet()) {
					List<List<Object>> payload = getPaylod(metricName, metricInstanceMap.get(metricName));
					// This need to be converted to JSON
					try {
						getJson(payload);
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		private List<List<Object>> getPaylod(String metricID, Set<Measure> measures) {
			List<List<Object>> measuresPayload = new ArrayList<>();

			for (Measure measure : measures) {
				List<Object> payload = new ArrayList<>();
				payload.add(sourceID);
				payload.add(metricID);
				payload.add(measure.getValue());
				payload.add(measure.getTimestamp());
				Map<String, String> measureTags = new HashedMap<>();
				measureTags.put("app_id", appID);
				payload.add(measureTags);

				measuresPayload.add(payload);
			}
			return measuresPayload;
		}

		private String getJson(Object object) throws JsonProcessingException {
			ObjectMapper mapper = new ObjectMapper();
			mapper.enable(SerializationFeature.INDENT_OUTPUT);
			String str = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
			System.out.println("REQUEST BODY : :" + str);
			return str;
		}
	}
}
