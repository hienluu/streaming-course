package streamingcourse.week2.json;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaJsonProducer {
    private static final String BOOTSTRAP_SERVER_LIST = "localhost:9092,localhost:9093,localhost:9094";
    private static final String KEY_SERIALIZER = StringSerializer.class.getName();
    private static final String VALUE_SERIALIZER = MobileUsageSerializer.class.getName();
    private static final long SLEEP_TIME_IN_MS = TimeUnit.SECONDS.toMillis(1);
    private static final int NUM_MSGS_TO_SEND = 20;
    private static final String KAFKA_TOPIC_TO_SEND_TO = "streaming.week2.mobile";

    public static void main(String[] args) {

        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaMobileProducer");

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);

        //Create a Kafka producer from configuration
        KafkaProducer<String, MobileUsage> simpleProducer = new KafkaProducer<String, MobileUsage>(kafkaProps);

        try {

            MobileUsage bobUsage = new MobileUsage();
            bobUsage.userName = "bob";
            bobUsage.bytesUsed = 25;
            bobUsage.timeStamp = Instant.now();

                //Create a producer Record
                ProducerRecord<String, MobileUsage> bobKafkaRecord =
                        new ProducerRecord<String, MobileUsage>(
                                KAFKA_TOPIC_TO_SEND_TO,    //Topic name
                                bobUsage.userName,  bobUsage);

            Future<RecordMetadata> response = simpleProducer.send(bobKafkaRecord);
            printRecordMetaData(response.get());

        }
        catch(Exception e) {
            System.out.println("Got exception: " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            simpleProducer.close();
        }
        System.out.println("Finish sending " + NUM_MSGS_TO_SEND + " messages");
    }

    private static void printRecordMetaData(RecordMetadata rm) {
        System.out.printf("topic: %s, partition: %d, offset: %d, timmestamp: %d, keySize: %d, valueSize: %d\n",
                rm.topic(), rm.partition(), rm.offset(), rm.timestamp(), rm.serializedKeySize(), rm.serializedValueSize());
    }

    private static void gsonQuickTest() {
        MobileUsage mobileUsage = new MobileUsage();
        mobileUsage.userName = "jb";
        mobileUsage.bytesUsed = 25;
        mobileUsage.timeStamp = Instant.now();

        System.out.println("mobileUsage: " + mobileUsage.toString());

        ObjectMapper objectMapper = JsonMapper.builder().build();

        objectMapper.findAndRegisterModules();

        try {

            // Getting organisation object as a json string
            String jsonStr = objectMapper.writeValueAsString(mobileUsage);

            // Displaying JSON String on console
            System.out.println(jsonStr);

            MobileUsage fromJson = objectMapper.readValue(jsonStr, MobileUsage.class);
            System.out.println("fromJson: " + fromJson.toString());

            String jsonStr2 = objectMapper.writeValueAsString(fromJson);

            // Displaying JSON String on console
            System.out.println(jsonStr2);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
