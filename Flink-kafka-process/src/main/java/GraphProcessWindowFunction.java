import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class GraphProcessWindowFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String, String, String>,Spans,String, TimeWindow> {
//    public static Map lastwindowtrace

    @Override
    public void process(String key, ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String, String, String>, Spans, String, TimeWindow>.Context context, Iterable<Tuple9<String, String, String, String, Long, Long, String, String, String>> iterable, Collector<Spans> collector) throws Exception {
        HashMap<String,Spans> SidToApi = new HashMap<>();
        HashMap<Spans,Long> ApiToCount = new HashMap<>();
        for(Tuple9<String, String, String, String, Long, Long, String, String, String> i:iterable){
            SidToApi.put(i.f1.trim(),new Spans(i.f3.trim(),i.f2.trim(),0,-1));
            if (i.f1.trim().equals(key.trim())){
                if (ApiToCount.containsKey(SidToApi.get(i.f1.trim()))){
                    ApiToCount.put(SidToApi.get(i.f1.trim()),ApiToCount.get(SidToApi.get(i.f1.trim()))+1);
                }else{
                    ApiToCount.put(SidToApi.get(i.f1.trim()),1L);
                }
            }
        }
        for(String sid:SidToApi.keySet()){
            if (!(sid.trim().equals(key.trim()))){
                String tmpparent = SidToApi.get(sid).getParent().trim();
                if (SidToApi.containsKey(tmpparent)){
                    String parentservice = SidToApi.get(tmpparent).getApi();
                    Spans tmpspan = SidToApi.get(sid);
                    tmpspan.setParent(parentservice);
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
        System.out.println(ApiToCount.keySet());
        for (Spans s: ApiToCount.keySet()){
            long tmpcount = ApiToCount.get(s);
            s.setCount(tmpcount);
            s.setTimestamp(context.window().getEnd());
            collector.collect(s);
        }
    }
}
