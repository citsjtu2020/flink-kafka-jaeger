public class AIOPSGraphSpan extends Object{
    public String end;
    public String start;
    public long count;
    public long timestamp;
    public String protocol;

    public String getProtocol() {
        return this.protocol;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public long getCount() {
        return this.count;
    }

    public String getEnd() {
        return this.end;
    }

    public String getStart() {
        return this.start;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public void setStart(String start) {
        this.start = start;
    }

    @Override
    public String toString() {
        return "AIOPSGraphSpan{" +
                "end='" + end + '\'' +
                ", start='" + start + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                ", protocol='" + protocol + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object span2) {
        if (this == span2){
            return true;
        }
        if (span2 instanceof AIOPSGraphSpan){
            AIOPSGraphSpan s2 = (AIOPSGraphSpan) span2;
//            && (this.count == s2.count)
            return ((this.end.equals(s2.end)) && (this.start.equals(s2.start)))&& (this.protocol.equals(s2.protocol));
        }else{
            return false;
        }
    }
}
