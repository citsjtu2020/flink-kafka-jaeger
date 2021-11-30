import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GraphAggProcessWindowFunction extends ProcessWindowFunction<Spans,Spans,String, TimeWindow> {
    @Override
    public void process(String key, ProcessWindowFunction<Spans, Spans, String, TimeWindow>.Context context, Iterable<Spans> iterable, Collector<Spans> collector) throws Exception {
        long count = 0;
        for(Spans s: iterable){
            count += s.getCount();
        }
//        return (spans.parent.trim()+";"+spans.api.trim()).trim();
        String[] api = key.split(";");
        Spans result = new Spans();
        result.setParent(api[0].trim());
        result.setApi(api[api.length-1].trim());
        result.setCount(count);
        result.setTimestamp(context.window().getEnd());
        collector.collect(result);
    }
}
