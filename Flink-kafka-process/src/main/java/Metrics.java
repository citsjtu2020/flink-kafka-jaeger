import lombok.val;

public class Metrics extends Object{
    public String api;
    public double value;
    public long timestamp;
    public String name;

    public Metrics(){}

    public Metrics(String api,double value,String name,long timestamp){
        this.api = api;
        this.value = value;
        this.name = name;
        this.timestamp = timestamp;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getApi() {
        return this.api;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getValue() {
        return this.value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object metric2) {
        if (this == metric2){
            return true;
        }
        if (metric2 instanceof Metrics){
            Metrics q2 = (Metrics) metric2;
            return ((this.name.equals(q2.name)) &&(this.api.equals(q2.api)) && (this.timestamp == q2.timestamp)
                     );
//            && this.value==(q2.value)
        }else{
            return false;
        }
    }


}
