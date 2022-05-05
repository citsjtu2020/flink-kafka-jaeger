import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

public class MySpanSource implements SourceFunction<Spans> {
    private long count = 0L;
    private boolean isRunning = true;
    private ArrayList<Spans> examples = new ArrayList<>();


    @Override
    public void run(SourceContext<Spans> sourceContext) throws Exception {
//        new Spans("http","0",100,1000L),
//                new Spans("http","0",100,1001L),
//                new Spans("http","0",100,1002L),
//                new Spans("http","0",100,1003L),
//                new Spans("http","0",100,1004L)
        examples.add(new Spans("http","0",100,1000L));
        examples.add(new Spans("http","0",100,1001L));
        examples.add(new Spans("http","0",100,1002L));
        examples.add(new Spans("http","0",100,1003L));
        examples.add(new Spans("http","0",100,1004L));
        int lens = examples.size();
        long basetime = examples.get(0).getTimestamp();
        while (isRunning){
//            JSONObject c = examples.get((int)count%lens);
            Spans ss = examples.get((int)count%lens);
            ss.setTimestamp(basetime+1000*count);
            sourceContext.collect(ss);
            count += 1;
            System.out.println(count);
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
