import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class GraphProcessWindowFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String, String, String>,Spans,String, TimeWindow> {
//    public static Map lastwindowtrace

    @Override // 输入属于同一个traceId的边
    public void process(String key, ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String, String, String>, Spans, String, TimeWindow>.Context context, Iterable<Tuple9<String, String, String, String, Long, Long, String, String, String>> iterable, Collector<Spans> collector) throws Exception {
        HashMap<String,Spans> SidToApi = new HashMap<>();
        HashMap<Spans,Long> ApiToCount = new HashMap<>(); // 记录每一条初始边出现了几次
        String _key=key.trim();
        for(Tuple9<String, String, String, String, Long, Long, String, String, String> i:iterable){
            String f1=i.f1.trim();
            SidToApi.put(f1,new Spans(i.f3.trim(),i.f2.trim(),0,-1));
            if (f1.equals(_key)){ // 如果这条边是trace的第一条边
                ApiToCount.put(SidToApi.get(f1),ApiToCount.getOrDefault(SidToApi.get(f1),0L)+1);
            }
        }
        for(String sid:SidToApi.keySet()){
            if (!(sid.trim().equals(_key))){ // 遍历每一条非初始边
                Spans tmpspan = SidToApi.get(sid);
                String tmpparent = tmpspan.getParent();
                if (SidToApi.containsKey(tmpparent)){
                    String parentservice = SidToApi.get(tmpparent).getApi();
                    tmpspan.setParent(parentservice); // 设置每一条非初始边的父节点的api
//                    SidToApi.put(sid,tmpspan); // 后面不需要span.parent了
                    ApiToCount.put(tmpspan,ApiToCount.getOrDefault(tmpspan,0L)+1);
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
