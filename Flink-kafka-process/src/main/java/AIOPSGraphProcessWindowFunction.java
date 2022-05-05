import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

public class AIOPSGraphProcessWindowFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String,String,String>,AIOPSSpans,String, TimeWindow> {
    public boolean pod_or_svc;
    public AIOPSGraphProcessWindowFunction(){
        pod_or_svc = false;
    }
    public AIOPSGraphProcessWindowFunction(boolean pod_or_svc){
        this.pod_or_svc = pod_or_svc;
    }
    @Override
    public void process(String key, ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String,String,String>, AIOPSSpans, String, TimeWindow>.Context context, Iterable<Tuple9<String, String, String, String, Long, Long, String,String,String>> iterable, Collector<AIOPSSpans> collector) throws Exception {
        HashMap<String,AIOPSSpans> SidToApi = new HashMap<>();
        HashMap<AIOPSSpans,Long> ApiToCount = new HashMap<>();
//        HashMap<AIOPSSpans, Double> ApiToDur = new HashMap<>();
//        HashMap<AIOPSSpans,Boolean> ApiKeep = new HashMap<>();
        for(Tuple9<String, String, String, String, Long, Long, String,String,String> i:iterable){
//                public AIOPSSpans(String api, String parent, long count, long timestamp, double rt, String protocol){
//                                    out.collect(new Tuple9<>(tid,sid,refer,outapi,starttime,dur,pod,pod_out_api,protocol));
            String outapi = i.f3.trim();
            if (this.pod_or_svc){
                outapi = i.f7.trim();
            }
            SidToApi.put(i.f1.trim(),new AIOPSSpans(outapi,i.f2.trim(),0,-1,i.f8));
            if (i.f2.trim().equals("client".trim())){
                AIOPSSpans tmpspan = SidToApi.get(i.f1.trim());
                if (ApiToCount.containsKey(tmpspan)){
                    ApiToCount.put(tmpspan,ApiToCount.get(SidToApi.get(i.f1.trim()))+1);
                }else {
                    tmpspan.setParent_type(tmpspan.getProtocol());
                    SidToApi.put(i.f1.trim(),tmpspan);
                    ApiToCount.put(tmpspan, 1L);
                }

//                ApiKeep.put(SidToApi.get(i.f1.trim()),true);
            }
        }
        for(String sid:SidToApi.keySet()){
            if (!(SidToApi.get(sid).getParent().trim().equals("client".trim()))){
                String tmpparent = SidToApi.get(sid).getParent().trim();
                if (SidToApi.containsKey(tmpparent)){
                    String parentservice = SidToApi.get(tmpparent).getApi();
                    AIOPSSpans tmpspan = SidToApi.get(sid);
                    tmpspan.setParent(parentservice);
                    String par_type = SidToApi.get(tmpparent).getProtocol();
                    tmpspan.setParent_type(par_type);
                    SidToApi.put(sid,tmpspan);
//                    ApiToCount.put(tmpspan)
                    if (ApiToCount.containsKey(tmpspan)){
                        ApiToCount.put(tmpspan,ApiToCount.get(tmpspan)+1);
                    }else {
                        ApiToCount.put(tmpspan, 1L);
                    }
                }
            }
        }
//        System.out.println(ApiToCount.keySet());
        for (AIOPSSpans s: ApiToCount.keySet()){
            long tmpcount = ApiToCount.get(s);
            s.setCount(tmpcount);
            s.setTimestamp(context.window().getEnd());
            collector.collect(s);
        }
    }
}
