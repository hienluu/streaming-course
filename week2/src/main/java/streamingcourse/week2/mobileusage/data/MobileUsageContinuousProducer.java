package streamingcourse.week2.mobileusage.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.week2.common.KJsonSerializer;
import streamingcourse.week2.common.MobileUsageCommonProperties;
import streamingcourse.week2.mobileusage.model.MobileUsage;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * This producer produces the mobile usages in a continuous manner until
 * the application is stopped.
 *
 * It is aimed to simulate real-time scenario where mobile usage events are continuous and unbounded.
 */
public class MobileUsageContinuousProducer {
    private static final String KEY_SERIALIZER = StringSerializer.class.getName();
    private static final String VALUE_SERIALIZER = KJsonSerializer.class.getName();

    public static final String MOBILE_USAGE_TOPIC = MobileUsageCommonProperties.MOBILE_USAGE_TOPIC;

    private static final Logger LOGGER = LogManager.getLogger(MobileUsageContinuousProducer.class.getName());

    public static void main(String[] args) {

        LOGGER.info(" =======================  MobileUsageContinuousProducer =========================");
        LOGGER.info("KAFKA_TOPIC_TO_SEND_TO: " + MOBILE_USAGE_TOPIC);
        LOGGER.info("KEY_SERIALIZER: " + KEY_SERIALIZER);
        LOGGER.info("VALUE_SERIALIZER: " + VALUE_SERIALIZER);
        LOGGER.info(" =======================  MobileUsageContinuousProducer =========================");


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

            MobileUsageGenerator generator  = new MobileUsageGenerator(5);
            Random random = new Random();
            LOGGER.info(" =======================  going in to forever loop =========================");
            while (true) {

                // between 1 and 7
                int numEventPerBatch = random.nextInt(7) + 1;
                LOGGER.info("producing " + numEventPerBatch + " event per batch");
                int count = 1;
                while (count <= numEventPerBatch) {
                    MobileUsage mobileUsage = generator.next();
                    //Create a producer Record
                    ProducerRecord<String, MobileUsage> kafkaRecord =
                            new ProducerRecord<String, MobileUsage>(
                                    MOBILE_USAGE_TOPIC,    //Topic name
                                    mobileUsage.userName, mobileUsage);

                    Future<RecordMetadata> response = simpleProducer.send(kafkaRecord);
                    printRecordMetaData(response.get());
                    count++;
                }

                try {
                    // sleeping tween 1 to 5 seconds
                    long randomSleepTimeMS = (random.nextInt(5) + 1 ) * 1000;
                    LOGGER.info("sleeping for " + randomSleepTimeMS);
                    Thread.sleep(randomSleepTimeMS);
                } catch (Exception e) {

                }
            }
        }
        catch(Exception e) {
            LOGGER.error("Got exception: " + e.getMessage(), e);
        }
        finally {
            simpleProducer.close();
        }

    }

    private static void printRecordMetaData(RecordMetadata rm) {
        System.out.printf("topic: %s, partition: %d, offset: %d, timmestamp: %d, keySize: %d, valueSize: %d\n",
                rm.topic(), rm.partition(), rm.offset(), rm.timestamp(), rm.serializedKeySize(), rm.serializedValueSize());
    }
}
