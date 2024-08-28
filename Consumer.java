package org.example;

import com.opencsv.CSVWriter;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.example.model.Week;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.awt.*;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class Consumer {
    private final static Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        log.info("Kafka Simple Consumer");

        String groupId = "my-application-bigdata-projekat";
        String topic = "forex-rates-topic";

        /// create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        try {
            CSVWriter writer = new CSVWriter(new FileWriter("C:\\Users\\matij\\OneDrive\\Desktop\\currency-data.csv", false));
            //            CSVWriter writer = new CSVWriter(new FileWriter("C:\\Users\\matij\\OneDrive\\Desktop\\rsd-to-eur-data.csv", false));
            // Write CSV header
            String[] header = {"Date", "Name", "Open", "High", "Low", "Close"};
            writer.writeNext(header);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                for(int i = 0; i < 65; i ++){
                    writer.writeNext(new String[]{"0", "0", "0", "0", "0"});
                }
                consumer.close();
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        /// poll new data
        while (true) {
            log.info("Polling... ");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(2000));
            String[] columns;
            String[] row;

            for (ConsumerRecord<String, String> record : consumerRecords) {
                columns = record.value().split("\t");
                row = new String[]{record.key(), columns[0], columns[1], columns[2], columns[3], columns[4]};
                writer.writeNext(row);
                writer.flush();
                log.info("Key: " + record.key() + ", Value: " + record.value()
                        + "\nPartition: " + record.partition() + "\nOffset: " + record.offset());
            }

        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

