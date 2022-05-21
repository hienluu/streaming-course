package streamingcourse.week2.json;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.time.Instant;

public class KafkaJsonProducer {
    public static void main(String[] args) {
        MobileUsage mobileUsage = new MobileUsage();
        mobileUsage.userName = "jb";
        mobileUsage.bytesUsed = 25;
        mobileUsage.timeStamp = Instant.now();

        System.out.println("mobileUsage: " + mobileUsage.toString());

        ObjectMapper objectMapper =  JsonMapper.builder()
                .build();

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
