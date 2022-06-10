package streamingcourse.week2.kafkastreams.stateful;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * When performing an average, we need two pieces of information:
 * - count
 * - total
 * <p>
 * For the mobile usage example, this simple class is used to hold the # of mobile usages and total used bytes
 */


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "countAndTotal"
})
public class CountAndTotal {
    private long count;
    private long total;

    public CountAndTotal() {
    }

    /**
     * Initialize with the given count and value
     * @param count
     * @param total
     */
    public CountAndTotal(long count, long total) {
        this.count = count;
        this.total = total;
    }

    public void incrementCountBy(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value can't be less than 0");
        }
        count += value;
    }

    public void incrementTotalBy(long value) {
        this.total += value;
    }

    @JsonProperty("count")
    public long getCount() {
        return count;
    }

    @JsonProperty("total")
    public long getTotal() {
        return total;
    }

    @JsonProperty("count")
    public void setCount(long count) {
        this.count = count;
    }

    @JsonProperty("total")
    public void setTotal(long total) {
        this.total = total;
    }
}
