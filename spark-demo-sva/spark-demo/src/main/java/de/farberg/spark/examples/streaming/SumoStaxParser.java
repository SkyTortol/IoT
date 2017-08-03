package de.farberg.spark.examples.streaming;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class SumoStaxParser {

	public static void sendToKafka(KafkaProducer<String, String> producer, String topic, String id, String message) {
		// Send the message
		producer.send(new ProducerRecord<String, String>(topic, id, message),
				(RecordMetadata metadata, Exception exception) -> {

					// Display some data about the message transmission
					if (metadata != null) {
						// System.out.println("Sent message(" + id + ", " + message + ") sent to
						// partition(" + metadata.partition() + "), "+ "offset(" + metadata.offset() +
						// ")");
					} else {
						exception.printStackTrace();
					}

				});
	}

	public static void main(String[] args) throws XMLStreamException, IOException, InterruptedException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "uoit.farberg.de:9092");
		props.put("group.id", "bla" + new Random().nextInt());
		props.put("client.id", "ich");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		String topic = "timmys";
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		XMLInputFactory inFactory = XMLInputFactory.newInstance();
		XMLEventReader eventReader = inFactory.createXMLEventReader(new FileInputStream("oshawa-mod.xml"));

		double lastTime = -1;
		double lastTimeXML = -1;

		while (eventReader.hasNext()) {
			XMLEvent event = eventReader.nextEvent();

			if (event.getEventType() == XMLEvent.START_ELEMENT) {
				StartElement startElement = event.asStartElement();

				if (startElement.getName().getLocalPart().equals("timestep")) {
					double currentTimeXML = Double
							.parseDouble(startElement.getAttributeByName(new QName("time")).getValue());

					if (lastTime > 0 && lastTimeXML > 0) {
						double now = System.currentTimeMillis();
						double elapsed = now - lastTime;
						double intendedTime = (currentTimeXML - lastTimeXML) * 1000.0;
						double toSleep = intendedTime - elapsed;

						if (toSleep > 0) {
							System.out.println("Timewarp to " + currentTimeXML + " sleeping for " + toSleep);
							Thread.sleep((long) toSleep);
						}
					}

					lastTime = System.currentTimeMillis();
					lastTimeXML = currentTimeXML;

				} else if (startElement.getName().getLocalPart().equals("vehicle")) {
					String vehicle = event.asStartElement().getAttributeByName(new QName("id")).getValue();
					double lat = Double
							.parseDouble(event.asStartElement().getAttributeByName(new QName("latitude")).getValue());
					double lon = Double
							.parseDouble(event.asStartElement().getAttributeByName(new QName("longitude")).getValue());

					String message = vehicle + "," + lat + "," + lon + "," + lastTimeXML;
					String id = "1";

					sendToKafka(producer, topic, id, message);
				}
			}

		}
	}
}
