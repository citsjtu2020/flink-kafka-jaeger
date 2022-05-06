import org.apache.kafka.common.protocol.types.Field;

public class LOGs extends Object{

    public String api;
    public String value;
    public long timestamp;
    public String name;

    public LOGs(){

    }
    public LOGs(String api, String value, long timestamp, String name){
        this.api = api;
        this.value = value;
        this.timestamp = timestamp;
        this.name = name;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getApi() {
        return this.api;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "LOGs{" +
                "api='" + api + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object logs2) {
        if (this == logs2){
            return true;
        }
        if (logs2 instanceof LOGs){
            LOGs q2 = (LOGs) logs2;
            return ((this.name.equals(q2.name)) &&(this.api.equals(q2.api)) && (this.timestamp == q2.timestamp)
                     && this.value.equals(q2.value));
        }else{
            return false;
        }
    }
}
