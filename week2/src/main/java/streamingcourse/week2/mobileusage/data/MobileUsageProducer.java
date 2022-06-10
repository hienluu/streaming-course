package streamingcourse.week2.mobileusage.data;


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

import java.util.concurrent.Future;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import streamingcourse.week2.common.KJsonSerializer;
import streamingcourse.week2.mobileusage.model.MobileUsage;

import static streamingcourse.common.KafkaCommonProperties.*;

/**
 * This Kafka producer produces Kafka records using custom type MobileUsage.
 * It uses the custom serializer called @{MobileUsageSerializer}
 */
public class MobileUsageProducer {

    private static final String KEY_SERIALIZER = StringSerializer.class.getName();
    private static final String VALUE_SERIALIZER = KJsonSerializer.class.getName();


    private static final int NUM_MSG_TO_SEND = 25;
    public static final String MOBILE_USAGE_TOPIC = "streaming.week2.mobile_usage";

    private static final Logger LOGGER = LogManager.getLogger(MobileUsageProducer.class.getName());
    public static void main(String[] args) {

        LOGGER.info(" =======================  MobileUsageProducer =========================");
        LOGGER.info("NUM_MSG_TO_SEND: " + NUM_MSG_TO_SEND);
        LOGGER.info("KAFKA_TOPIC_TO_SEND_TO: " + MOBILE_USAGE_TOPIC);
        LOGGER.info("KEY_SERIALIZER: " + KEY_SERIALIZER);
        LOGGER.info("VALUE_SERIALIZER: " + VALUE_SERIALIZER);
        LOGGER.info(" =======================  MobileUsageProducer =========================");


        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, MobileUsageProducer.class.getName());
        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);

        //Create a Kafka producer from configuration
        KafkaProducer<String, MobileUsage> simpleProducer = new KafkaProducer<String, MobileUsage>(kafkaProps);

        try {
            MobileUsageGenerator generator  = new MobileUsageGenerator(15);
            int rowCount = 0;
            while (rowCount < NUM_MSG_TO_SEND) {
                MobileUsage mobileUsage = generator.next();

                //Create a producer Record
                ProducerRecord<String, MobileUsage> kafkaRecord =
                        new ProducerRecord<String, MobileUsage>(
                                MOBILE_USAGE_TOPIC,    //Topic name
                                mobileUsage.userName,  mobileUsage);

                Future<RecordMetadata> response = simpleProducer.send(kafkaRecord);
                printRecordMetaData(response.get());

                rowCount++;
            }
        }
        catch(Exception e) {
            LOGGER.error("Got exception: " + e.getMessage(), e);
        }
        finally {
            simpleProducer.close();
        }
        LOGGER.info("Finish sending " + NUM_MSG_TO_SEND + " messages");
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
