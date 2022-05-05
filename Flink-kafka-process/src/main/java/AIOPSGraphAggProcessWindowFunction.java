import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AIOPSGraphAggProcessWindowFunction extends ProcessWindowFunction<AIOPSSpans,AIOPSGraphSpan,String, TimeWindow> {
    @Override
    public void process(String key, ProcessWindowFunction<AIOPSSpans, AIOPSGraphSpan, String, TimeWindow>.Context context, Iterable<AIOPSSpans> iterable, Collector<AIOPSGraphSpan> collector) throws Exception {
        long count = 0;
        for(AIOPSSpans s: iterable){
            count += s.getCount();
        }
//        return (spans.parent.trim()+";"+spans.api.trim()).trim();
        String[] api = key.trim().split(";");
        AIOPSGraphSpan result = new AIOPSGraphSpan();
//        result.setParent(api[0].trim());
//        result.setApi(api[api.length-1].trim());
//        result.setCount(count);
        result.setCount(count);
        result.setEnd(api[1].trim());
        result.setProtocol(api[api.length-1].trim());
        result.setStart(api[0].trim());
        result.setTimestamp(context.window().getEnd());
        System.out.println(result);
        collector.collect(result);
        System.out.println("collect down!!!!");
    }
}
