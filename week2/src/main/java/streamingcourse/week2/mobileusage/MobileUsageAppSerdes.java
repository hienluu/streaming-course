package streamingcourse.week2.mobileusage;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import streamingcourse.week2.common.KJsonDeserializer;
import streamingcourse.week2.common.KJsonSerializer;

import java.util.HashMap;
import java.util.Map;

public class MobileUsageAppSerdes extends Serdes {
    static final class MobileUsageSerde2 extends WrapperSerde<MobileUsage> {
        MobileUsageSerde2() {
            super(new KJsonSerializer<>(), new KJsonDeserializer<>());
        }
    }

    public static Serde<MobileUsage> MobileUsage() {
        MobileUsageSerde2 serde = new MobileUsageSerde2();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(KJsonDeserializer.VALUE_CLASS_NAME_CONFIG, MobileUsage.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}
