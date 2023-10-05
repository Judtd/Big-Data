package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducer {

    private static final String TOPIC_NAME = "song-topic";
    private static final String FILE_PATH = "/Users/Juddy/Desktop/kafkaProject/data/Spotify_Youtube.csv";
    private static final int INTERVAL_MS = 50;
    private static final int RECORDS_PER_MINUTE = 10;

    public static void main(String[] args) throws IOException, CsvValidationException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH));
             CSVReader csvReader = new CSVReader(reader)) {

            String[] headers = csvReader.readNext();
            String[] nextLine;

            while ((nextLine = csvReader.readNext()) != null) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 0; i < headers.length; i++) {
                    jsonObject.put(headers[i], nextLine[i]);
                }
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(TOPIC_NAME, null, jsonObject.toString());

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Sent: " + jsonObject.toString());
                        } else {
                            System.err.println("Error sending record: " + exception.getMessage());
                        }
                    }
                });

                TimeUnit.MILLISECONDS.sleep(INTERVAL_MS);
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
