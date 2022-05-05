import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

public class AIOPSPodTraceProcessWindowFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String,String, String>,QPSs,String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Tuple9<String, String, String, String, Long, Long, String, String, String>, QPSs, String, TimeWindow>.Context context, Iterable<Tuple9<String, String, String, String, Long, Long, String, String, String>> iterable, Collector<QPSs> collector) throws Exception {
     long count = 0;
        double mean = 0.0;
        ArrayList<Long> DurList = new ArrayList<>();
        for(Tuple9<String, String, String, String, Long, Long, String,String, String> in : iterable){
            DurList.add(in.f5);
            count++;
            mean += in.f5;
        }
        mean = mean / count;
        Collections.sort(DurList);
        double std = 0.0;
        for (long dur : DurList){
            std += (dur - mean)*(dur-mean);
        }
        std = Math.sqrt(std);
        std = std / count;
        double mindur = (double)DurList.get(0);
        double maxdur = (double)DurList.get(DurList.size()-1);
        //P50
        int tmp = Math.min((int)Math.floor(DurList.size()*0.5),DurList.size()-1);
        double p50dur = (double)DurList.get(tmp);
//        //P90
//        tmp  = Math.min((int)Math.floor(DurList.size()*0.9),DurList.size()-1);
//        double p90dur = (double)DurList.get(tmp);
        //P95
        tmp  = Math.min((int)Math.floor(DurList.size()*0.95),DurList.size()-1);
        double p95dur = (double)DurList.get(tmp);
        //P99
        tmp  = Math.min((int)Math.floor(DurList.size()*0.99),DurList.size()-1);
        double p99dur = (double)DurList.get(tmp);
        QPSs out = new QPSs(s,mean,mindur,maxdur,std,p50dur,p95dur,p99dur,count,context.window().getEnd());
//
        System.out.println("QPS out:"+out.toString());
        collector.collect(out);
    }
}
