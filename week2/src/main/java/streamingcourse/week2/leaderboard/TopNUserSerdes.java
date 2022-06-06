package streamingcourse.week2.leaderboard;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import streamingcourse.week2.common.KJsonDeserializer;
import streamingcourse.week2.common.KJsonSerializer;

import java.util.HashMap;
import java.util.Map;

public class TopNUserSerdes {
    static final class UserTotalUsageSerde extends Serdes.WrapperSerde<UserTotalUsage> {
        UserTotalUsageSerde() {
            super(new KJsonSerializer<>(), new KJsonDeserializer<>());
        }
    }

    public static Serde<UserTotalUsage> UserTotalUsage() {
        UserTotalUsageSerde serde = new UserTotalUsageSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(KJsonDeserializer.VALUE_CLASS_NAME_CONFIG, UserTotalUsage.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class TopNUsersSerde extends Serdes.WrapperSerde<TopNUsers> {
        TopNUsersSerde() {
            super(new KJsonSerializer<>(), new KJsonDeserializer<>());
        }
    }

    public static Serde<TopNUsers> TopNUsers() {
        TopNUsersSerde serde = new TopNUsersSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(KJsonDeserializer.VALUE_CLASS_NAME_CONFIG, TopNUsers.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}
