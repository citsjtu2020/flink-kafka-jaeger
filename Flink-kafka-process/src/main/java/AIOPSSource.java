import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.protocol.types.Field;

import java.io.*;
import java.util.ArrayList;

public class AIOPSSource implements SourceFunction<String> {

    private long count = 0L;
    private boolean isRunning = true;
    private ArrayList<JSONObject> SourceLisst = new ArrayList<>();

//    public AIOPSSource(){
//
//    }

    public static ArrayList<JSONObject> toArrayByInputStreamReader1(String name) {
        // 使用ArrayList来存储每行读取到的字符串
        ArrayList<JSONObject> arrayList = new ArrayList<>();
        try {
            File file = new File(name);
            InputStreamReader inputReader = new InputStreamReader(new FileInputStream(file));
            BufferedReader bf = new BufferedReader(inputReader);
            // 按行读取字符串
            String str;
            while ((str = bf.readLine()) != null) {
//                System.out.println(str);
                JSONObject tmpjson = JSON.parseObject(str.trim());
                if (!(tmpjson.containsKey("log_id"))){
                    arrayList.add(tmpjson);
                }
//                arrayList.add(str);
            }
            bf.close();
            inputReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        // 对ArrayList中存储的字符串进行处理
//        int length = arrayList.size();
//        String[] array = new String[length];
//        for (int i = 0; i < length; i++) {
//            String s = arrayList.get(i);
//            array[i] = s.trim();
//        }
//        // 返回数组
        return arrayList;
    }


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
//        array = toArrayByInputStreamReader1("")
//        /home/huaqin/data0421_01.txt

//        {"timestamp":"1650517025591","cmdb_id":"frontend-0",
//        "span_id":"e75db2d334118d6b",
//        "trace_id":"8915ad35a681682672dd003ccd6b5af2",
//        "duration":"2244","type":"rpc","status_code":"0",
//        "operation_name":"hipstershop.CurrencyService/Convert",
//        "parent_span":"1ae5793e9e0bbf71"}
//        SourceLisst = toArrayByInputStreamReader1(哦名data0421_01.txt");
        SourceLisst = toArrayByInputStreamReader1("/home/k8smaster/data0421_01.txt");
        int lens = SourceLisst.size();
        long basetime = Long.parseLong(SourceLisst.get(0).get("timestamp").toString());
        long mintime = Long.parseLong(SourceLisst.get(0).get("timestamp").toString());
        long maxtime = Long.parseLong(SourceLisst.get(0).get("timestamp").toString());
        long nowtime = Long.parseLong(SourceLisst.get(0).get("timestamp").toString());
        while (isRunning){
            JSONObject c = SourceLisst.get((int)count%lens);
            if (count % lens == 0 && count > 0){
                nowtime = basetime + (count / lens)*(maxtime - basetime);
            }
            if (count % 300 == 0){
                System.out.println(c);
            }
            // System.out.println(count);

            long tmp_timestamp = Long.parseLong(c.get("timestamp").toString());
            tmp_timestamp = nowtime + (tmp_timestamp - basetime);
            c.put("timestamp",tmp_timestamp);
            sourceContext.collect(c.toString());
            count += 1;
//           火热
            Thread.sleep(3);

//            System.out.println(count);
//            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
