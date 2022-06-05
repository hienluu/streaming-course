package streamingcourse.week2.mobileusage.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.week2.common.KJsonSerializer;
import streamingcourse.week2.mobileusage.model.DeptInfo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

public class DepartmentInfoProducer {

    private static final String KEY_SERIALIZER = IntegerSerializer.class.getName();
    private static final String VALUE_SERIALIZER = KJsonSerializer.class.getName();

    public static final String KAFKA_TOPIC_TO_SEND_TO = "streaming.week2.department";

    private static final Logger LOGGER = LogManager.getLogger(DepartmentInfoProducer.class.getName());
    public static void main(String[] args) {

        LOGGER.info(" =======================" + DepartmentInfoProducer.class.getName() + " =========================");
        LOGGER.info("KAFKA_TOPIC_TO_SEND_TO: " + KAFKA_TOPIC_TO_SEND_TO);
        LOGGER.info("KEY_SERIALIZER: " + KEY_SERIALIZER);
        LOGGER.info("VALUE_SERIALIZER: " + VALUE_SERIALIZER);
        LOGGER.info(" =======================" + DepartmentInfoProducer.class.getName() + " =========================");


        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, DepartmentInfoProducer.class.getName());
        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);

        //Create a Kafka producer from configuration
        KafkaProducer<Integer, DeptInfo> simpleProducer = new KafkaProducer<Integer, DeptInfo>(kafkaProps);

        try {
            DepartmentInfoGenerator generator  = DepartmentInfoGenerator.getInstance();
            List<DeptInfo> deptInfoList = generator.getDeptInfoList();
            for (DeptInfo deptInfo : deptInfoList) {

                //Create a producer Record
                ProducerRecord<Integer, DeptInfo> kafkaRecord =
                        new ProducerRecord<>(
                                KAFKA_TOPIC_TO_SEND_TO,    //Topic name
                                deptInfo.deptIt,  deptInfo);

                Future<RecordMetadata> response = simpleProducer.send(kafkaRecord);
                printRecordMetaData(response.get());
            }
            LOGGER.info("Finish sending " + deptInfoList.size() + " messages");
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
