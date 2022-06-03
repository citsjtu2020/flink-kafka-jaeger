import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GraphAggProcessWindowFunction extends ProcessWindowFunction<Spans,AIOPSGraphSpan,String, TimeWindow> {
    @Override
    public void process(String key, ProcessWindowFunction<Spans, AIOPSGraphSpan, String, TimeWindow>.Context context, Iterable<Spans> iterable, Collector<AIOPSGraphSpan> collector) throws Exception {
        long count = 0;
        for(Spans s: iterable){
            count += s.getCount();
        }
//        return (spans.parent.trim()+";"+spans.api.trim()).trim();
        String[] api = key.split(";");
        AIOPSGraphSpan result = new AIOPSGraphSpan();
//        result.setParent(api[0].trim());
//        result.setApi(api[api.length-1].trim());

        result.setCount(count);
        result.setEnd(api[1].trim());
        result.setProtocol(api[api.length-1].trim());
        result.setStart(api[0].trim());
//        System.out.println(result);
        result.setTimestamp(context.window().getEnd());
        collector.collect(result);
//        System.out.println("collect down!!!!");
    }
}
