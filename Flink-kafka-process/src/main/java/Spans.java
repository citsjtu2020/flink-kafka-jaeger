import java.util.Objects;

public class Spans extends Object{
//    public String tid;
    public String api;
    public String parent;
    public long count;
    public long timestamp;
    public String protocol;
    public String parent_type;
//    public
    public Spans(){

    }

    public long getCount() {
        return this.count;
    }

    public String getApi() {
        return this.api;
    }

    public String getParent() {
        return this.parent;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getParent_type() {
        return this.parent_type;
    }

    public void setParent_type(String parent_type) {
        this.parent_type = parent_type;
    }

    public String getProtocol() {
        return this.protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Spans(String api, String parent, long count, long timestamp,String protocol){
        this.api = api;
        this.parent = parent;
        this.count = count;
        this.timestamp = timestamp;
        this.protocol = protocol;
        this.parent_type = protocol;
    }

    @Override
    public boolean equals(Object span2) {
        if (this == span2){
            return true;
        }
        if (span2 instanceof Spans){
            Spans s2 = (Spans) span2;
//            return ((this.api.equals(s2.api)) && (this.parent.equals(s2.parent)) && (this.count == s2.count));
            return ((this.api.equals(s2.api)) && (this.parent.equals(s2.parent)))&& (this.protocol.equals(s2.protocol)&& (this.parent_type.equals(s2.parent_type)));
        }else{
            return false;
        }
    }

    @Override
    public String toString() {
        return "Spans{" +
                "api='" + api + '\'' +
                ", parent='" + parent + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                ", protocol='" + protocol + '\'' +
                ", parent_type='" + parent_type + '\'' +
                '}';
    }
}
