package org.example;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.Week;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProducerEURToEuropeanCurrencies {


        private final static Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

        public static void main(String[] args) {
            log.info("Kafka Simple Producer");

            String[] isoCodes = {"HUF", "SEK", "GBP", "CHF"};
            /// create Producer Properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            for(String isoCode : isoCodes){
                String url = "https://www.alphavantage.co/query?function=FX_WEEKLY&from_symbol=EUR&to_symbol=" + isoCode +  "&apikey=YZ728BGP7767IBG6";
                OkHttpClient client = new OkHttpClient();

                Request request = new Request.Builder()
                        .url(url)
                        .build();

                String json = "";

                try (Response response = client.newCall(request).execute()) {
                    if (response.isSuccessful()) {
                        ResponseBody responseBody = response.body();
                        if (responseBody != null) {
                            json = responseBody.string();
                        }
                    } else {
                        System.err.println("Error: " + response.code() + " - " + response.message());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                Gson gson = new Gson();
                Week forexWeeklyPrices = gson.fromJson(json, Week.class);

                Map<String, Week.WeeklyRate> timeSeries = forexWeeklyPrices.getTimeSeries();

                /// create Producer
                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


                for (Map.Entry<String, Week.WeeklyRate> entry : timeSeries.entrySet()) {
                    String date = entry.getKey();
                    Week.WeeklyRate forexData = entry.getValue();

                    String topic = "forex-rates-topic";
                    String key = date;
                    String value = "EUR to " + isoCode + "\t" + forexData.toString();
                    /// create Producer record
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                    /// send data
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                log.info("Received message " +
                                        "\nTopic " + metadata.topic() +
                                        "\nPartition " + metadata.partition() +
                                        "\nOffset " + metadata.offset());
                            } else {
                                log.info("Error while producing ", exception);
                            }
                        }
                    });
                }

                producer.flush();

                producer.close();
            }
        }


}
