import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class GraphProcessWindowFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Long, Double, String, String, String>,Spans,String, TimeWindow> {
//    public static Map lastwindowtrace
    public boolean pod_or_svc;
    public GraphProcessWindowFunction(){
        pod_or_svc = false;
    }

    @Override
    public void process(String key, ProcessWindowFunction<Tuple9<String, String, String, String, Long, Double, String, String, String>, Spans, String, TimeWindow>.Context context, Iterable<Tuple9<String, String, String, String, Long, Double, String, String, String>> iterable, Collector<Spans> collector) throws Exception {
        HashMap<String,Spans> SidToApi = new HashMap<>();
        HashMap<Spans,Long> ApiToCount = new HashMap<>();
        for(Tuple9<String, String, String, String, Long, Double, String, String, String> i:iterable){
            String outapi = i.f3.trim();
            if (this.pod_or_svc){
                outapi = i.f7.trim();
            }
            SidToApi.put(i.f1.trim(),new Spans(i.f3.trim(),i.f2.trim(),0,-1,i.f8.trim()));
            if (i.f1.trim().equals(key.trim()) || (i.f2.trim().equals("client".trim()))){
                Spans tmpspan = SidToApi.get(i.f1.trim());
                if (tmpspan.getParent_type().equals("")){
                    tmpspan.setParent_type(tmpspan.getProtocol());
                }
//                SidToApi.get(i.f1.trim()))
                if (ApiToCount.containsKey(tmpspan)){
//                    SidToApi.get(i.f1.trim())
//                    SidToApi.get(i.f1.trim())
                    ApiToCount.put(tmpspan,ApiToCount.get(tmpspan)+1);
                }else{
//                    ApiToCount.put(SidToApi.get(i.f1.trim()),1L);
                    tmpspan.setParent_type(tmpspan.getProtocol());
                    SidToApi.put(i.f1.trim(),tmpspan);
                    ApiToCount.put(tmpspan, 1L);
                }
            }
        }
        for(String sid:SidToApi.keySet()){
            if (!(sid.trim().equals(key.trim())) && !(SidToApi.get(sid).getParent().trim().equals("client".trim()))){
                String tmpparent = SidToApi.get(sid).getParent().trim();
                if (SidToApi.containsKey(tmpparent)){
                    String parentservice = SidToApi.get(tmpparent).getApi();
                    Spans tmpspan = SidToApi.get(sid);
                    tmpspan.setParent(parentservice);
                    String par_type = SidToApi.get(tmpparent).getProtocol();
                    tmpspan.setParent_type(par_type);
                    SidToApi.put(sid,tmpspan);
//                    ApiToCount.put(tmpspan)
                    if (ApiToCount.containsKey(tmpspan)){
                        ApiToCount.put(tmpspan,ApiToCount.get(tmpspan)+1);
                    }else{
                        ApiToCount.put(tmpspan,1L);
                    }
                }
            }
        }
//        System.out.println(ApiToCount.keySet());
        for (Spans s: ApiToCount.keySet()){
            long tmpcount = ApiToCount.get(s);
            s.setCount(tmpcount);
            s.setTimestamp(context.window().getEnd());
            collector.collect(s);
        }
    }
}
