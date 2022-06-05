package streamingcourse.week2.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class MyJsonSerializer {
    public static MyJsonSerializer build() {
        return new MyJsonSerializer();
    }

    private ObjectMapper objectMapper;
    private MyJsonSerializer() {
        objectMapper =
                JsonMapper.builder()
                        .build().findAndRegisterModules();
    }

    public Object fromJson(String jsonStr, Class clazz) {
        try {
            return objectMapper.readValue(jsonStr, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String toJson(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
