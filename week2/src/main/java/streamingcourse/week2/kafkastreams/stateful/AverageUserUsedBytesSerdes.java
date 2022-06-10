package streamingcourse.week2.kafkastreams.stateful;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import streamingcourse.week2.common.KJsonDeserializer;
import streamingcourse.week2.common.KJsonSerializer;


import java.util.HashMap;
import java.util.Map;

public class AverageUserUsedBytesSerdes {
    static final class CountAndTotalSerde extends Serdes.WrapperSerde<CountAndTotal> {
        CountAndTotalSerde() {
            super(new KJsonSerializer<>(), new KJsonDeserializer<>());
        }
    }

    public static Serde<CountAndTotal> CountAndValue() {
        CountAndTotalSerde serde = new CountAndTotalSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(KJsonDeserializer.VALUE_CLASS_NAME_CONFIG, CountAndTotal.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
