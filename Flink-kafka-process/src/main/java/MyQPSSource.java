import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;


public class MyQPSSource implements SourceFunction<QPSs> {
    private long count = 0L;
    private boolean isRunning = true;
    private ArrayList<QPSs> examples = new ArrayList<>();

    @Override
    public void run(SourceContext<QPSs> sourceContext) throws Exception {
//        1633607526581
//        QPSs(String api,double meandur,double mindur,double maxdur,double std,double p50dur,double p95dur,double p99dur,long count,long timestamp)
        examples.add(new QPSs("http", 100, 10, 250, 5.4, 120, 180, 190, 1000, Long.parseLong("1633607525581".trim())));
        examples.add(new QPSs("search-search.search", 100, 10, 250, 5.4, 120, 180, 190, 1000, Long.parseLong("1633607526581".trim())));
        int lens = examples.size();
        long basetime = examples.get(0).getTimestamp();
        while (isRunning) {
            QPSs c = examples.get((int) count % lens);
            c.setTimestamp(basetime + 1000 * count);
            sourceContext.collect(c);
            count += 1;
            System.out.println(count);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
