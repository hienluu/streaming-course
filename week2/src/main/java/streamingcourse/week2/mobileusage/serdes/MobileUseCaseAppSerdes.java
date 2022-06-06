package streamingcourse.week2.mobileusage.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import streamingcourse.week2.common.KJsonDeserializer;
import streamingcourse.week2.common.KJsonSerializer;
import streamingcourse.week2.mobileusage.model.DeptInfo;
import streamingcourse.week2.mobileusage.model.MobileUsage;

import java.util.HashMap;
import java.util.Map;

public class MobileUseCaseAppSerdes extends Serdes {
    static final class MobileUsageSerde2 extends WrapperSerde<MobileUsage> {
        MobileUsageSerde2() {
            super(new KJsonSerializer<>(), new KJsonDeserializer<>());
        }
    }

    static final class DepartmentInfoSerde extends WrapperSerde<DeptInfo> {
        DepartmentInfoSerde() {
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

    public static Serde<DeptInfo> DepartmentInfo() {
        DepartmentInfoSerde serde = new DepartmentInfoSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(KJsonDeserializer.VALUE_CLASS_NAME_CONFIG, DeptInfo.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
