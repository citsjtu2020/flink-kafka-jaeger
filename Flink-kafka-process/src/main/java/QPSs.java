import java.util.Objects;

public class QPSs extends Object{

//    Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long> out = new Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long>(
//                s,mean,mindur,maxdur,std,p50dur,p95dur,p99dur,count,context.window().getEnd()
    public String api;
    public double meandur;
    public double mindur;
    public double maxdur;
    public double std;
    public double p50dur;
    public double p95dur;
    public double p99dur;
    public long count;
    public long timestamp;

    public QPSs(){

    }

    public QPSs(String api,double meandur,double mindur,double maxdur,double std,double p50dur,double p95dur,double p99dur,long count,long timestamp){
        this.api = api;
        this.maxdur = maxdur;
        this.meandur = meandur;
        this.mindur = mindur;
        this.std = std;
        this.p50dur = p50dur;
        this.p95dur = p95dur;
        this.p99dur = p99dur;
        this.count = count;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getApi() {
        return this.api;
    }

    public long getCount() {
        return this.count;
    }

    public double getMaxdur() {
        return this.maxdur;
    }

    public double getMeandur() {
        return this.meandur;
    }

    public double getMindur() {
        return this.mindur;
    }

    public double getP50dur() {
        return this.p50dur;
    }

    public double getP95dur() {
        return this.p95dur;
    }

    public double getP99dur() {
        return this.p99dur;
    }

    public double getStd() {
        return this.std;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setMaxdur(double maxdur) {
        this.maxdur = maxdur;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setMeandur(double meandur) {
        this.meandur = meandur;
    }

    public void setMindur(double mindur) {
        this.mindur = mindur;
    }

    public void setP50dur(double p50dur) {
        this.p50dur = p50dur;
    }

    public void setP95dur(double p95dur) {
        this.p95dur = p95dur;
    }

    public void setP99dur(double p99dur) {
        this.p99dur = p99dur;
    }

    public void setStd(double std) {
        this.std = std;
    }

    @Override
    public String toString() {
        return "QPSs{" +
                "api='" + api + '\'' +
                ", meandur=" + meandur +
                ", mindur=" + mindur +
                ", maxdur=" + maxdur +
                ", std=" + std +
                ", p50dur=" + p50dur +
                ", p95dur=" + p95dur +
                ", p99dur=" + p99dur +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object qps2) {
        if (this == qps2){
            return true;
        }
        if (qps2 instanceof QPSs){
            QPSs q2 = (QPSs) qps2;
            return ((this.api.equals(q2.api)) && (this.timestamp == q2.timestamp)
                    && (this.count == q2.count) && (this.meandur == q2.meandur)
                    && (this.maxdur == q2.maxdur) && (this.mindur == q2.mindur)
                    && (this.p99dur == q2.p99dur) && (this.p95dur == q2.p95dur)
                    && (this.p50dur == q2.p50dur) && this.std == q2.std);
        }else{
            return false;
        }
    }


}
