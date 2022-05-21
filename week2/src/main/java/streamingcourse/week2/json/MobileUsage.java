package streamingcourse.week2.json;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.StringJoiner;


public class MobileUsage {
    public String userName;
    public long bytesUsed;
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            timezone = "UTC")
    public Instant timeStamp;

    public MobileUsage() {

    }

    public MobileUsage(Instant ts, String userName, long bytesUsed) {
        this.timeStamp = ts;
        this.userName = userName;
        this.bytesUsed = bytesUsed;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "[", "]");

        joiner.add("userName=" + userName);
        joiner.add("bytesUsed=" + bytesUsed);
        joiner.add("timeStamp=" + timeStamp);

        return joiner.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MobileUsage that = (MobileUsage) o;
        return userName.equals(that.userName) && bytesUsed == that.bytesUsed && timeStamp.equals(that.timeStamp);
    }

}
