import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Iterator;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) throws Exception {
//        Flink
        //备选方案：最熟悉的就是直接解析成字符串，然后使用fastjson进行解析，速度也比较快，如果出现bug就改用简单的方法实现
//        String kafkaserver = args[0].trim();
//        String topic = args[1].trim();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
//        "10.100.233.199:9092"
//        properties.setProperty("bootstrap.servers",kafkaserver);
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        System.out.println("start the consumer");
//        "test"
//        System.out.println(kafkaserver);
//        System.out.println(topic);
//        topic
        FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>("test",new JSONKeyValueDeserializationSchema(true),properties);
        consumer.setStartFromGroupOffsets();
        DataStream<ObjectNode> streamSource = env.addSource(consumer);
//        DataStream<ObjectNode> streamSource = env.addSource(new MySource());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        streamSource.print();
//        outkey.set(tid);
//        ,String
//        ,String
//        String,
//        outvalue.set(sid+","+refer+","+service+","+operation+","+starttime+","+dur+","+pod+","+endpoint+","+protocol);
        DataStream<Tuple9<String,String,String,String,Long,Long,String,String,String>> sourcecsv =
        streamSource.process(new ProcessFunction<ObjectNode, Tuple9<String,String,String,String,Long,Long,String,String,String>>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<Tuple9<String,String,String,String,Long,Long,String,String,String>> out) throws Exception {
//                String tid = object.getString("traceID");
//        String sid = object.getString("spanID");
//        String protocol = "";
//        String pod = "";
//        String endpoint = "";
//        int dur = object.getInteger("duration");
//        JSONObject process = object.getJSONObject("process");
//        String service = process.getString("serviceName");
//        Set<String> proset = process.keySet();
//                 JsonNode phone_num = value.get("value").get("phone_num");
//                System.out.println("phone_num"+phone_num);
//                JsonNode name = value.get("value").get("name");
//                System.out.println("name"+name);
//                JsonNode age = value.get("value").get("age");
//                System.out.println("age"+age);
//                JsonNode sex = value.get("value").get("sex");
//                System.out.println("sex"+sex);
                String tid = value.get("value").get("traceID").asText().trim();
                String sid = value.get("value").get("spanID").asText().trim();
                long dur = value.get("value").get("duration").asLong();
                JsonNode process = value.get("value").get("process");
                String service = process.get("serviceName").asText().trim();
                String pod = "";
                String endpoint = "";
                String protocol = "";
//                component
                if (process.has("tags")) {
                    JsonNode tags = process.get("tags");
                    for (Iterator<JsonNode> elements = tags.elements(); elements.hasNext();){
                        JsonNode next = elements.next();
                        String aim = next.get("key").asText().trim();
                        if (aim.trim().equals("hostname")){
//                    System.out.println(jtag);
                            pod = next.get("value").asText();
//                    break;
                        }
                        if (aim.trim().equals("ip")){
                            endpoint = next.get("value").asText();
                        }
                        if ((!endpoint.isEmpty()) && (!pod.isEmpty())){
                            break;
                        }
                    }
                }
                JsonNode spantags = value.get("value").get("tags");
                for(Iterator<JsonNode> elements = spantags.elements();elements.hasNext();){
                    JsonNode next = elements.next();
                    String aim = next.get("key").asText().trim();
                    if (aim.trim().equals("component")){
//                System.out.println(jtag);
                        protocol = next.get("value").asText().trim();
                        break;
                    }
                }
                long starttime = value.get("value").get("startTimeMillis").asLong();
                String operation = value.get("value").get("operationName").asText().trim();
                String refer = "0";
                boolean childs = false;
                String outapi = "";
//                object.getJSONArray("references").isEmpty()== false
                if (value.get("value").get("references").isEmpty() == false){
//                    value.get("value").get("references").
                    refer = value.get("value").get("references").get(0).get("spanID").asText().trim();
                    childs = true;
                }
                operation = operation.replace('/','-');
                if (!childs){
                    outapi = "http".trim();
                }else{
                    outapi = service.trim()+operation;
                }
//                System.out.println("trace: "+tid);
//                System.out.println("span: "+sid);
//                System.out.println("duration: "+dur);
//                service,operation
                out.collect(new Tuple9<>(tid,sid,refer,outapi,starttime,dur,pod,endpoint,protocol));

//                outkey.set(tid);
//                outvalue.set(sid+","+refer+","+service+","+operation+","+starttime+","+dur+","+pod+","+endpoint+","+protocol);
//                context.write(outkey,outvalue);
            }
        }

//            @Override
//            public void processElement(ObjectNode value, Context ctx, Collector<String> out) throws Exception {
//                JsonNode json1 = value.get("value").get("phone_num");
//                for(Iterator<JsonNode> elements = json1.elements();elements.hasNext();){
//                    JsonNode next = elements.next();
//                    System.out.println(next);
//                    for(Iterator<Map.Entry<String, JsonNode>> next0 = next.fields();next0.hasNext();){
//                        Map.Entry<String, JsonNode> entry = next0.next();
//                        System.out.println(entry.getKey());
//                        System.out.println(entry.getValue());
//                    }
//                }
//            }
//        }
        );
//        sourcecsv.print();

//      out.collect(new Tuple10<>(tid,sid,refer,service,operation,starttime,dur,pod,endpoint,protocol));

//        stream.print();
        SingleOutputStreamOperator<Tuple9<String, String, String, String, Long, Long, String, String, String>> source1 =  sourcecsv.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple9<String, String, String, String, Long, Long, String, String, String>>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Tuple9<String, String, String, String, Long, Long, String, String, String> trace) {
                return trace.f4;
            }
        });
//Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long>
        DataStream<QPSs> out1 = source1
                .keyBy(t->t.f3)
                .timeWindow(Time.milliseconds(30*1000)).process(new TraceProcessWindowFunction());

        DataStream<Spans> out2 = source1.keyBy(t->t.f0).timeWindow(Time.milliseconds(30*1000)).process(new GraphProcessWindowFunction());
        out2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Spans>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Spans spans) {
                return spans.timestamp;
            }
        }).keyBy(new KeySelector<Spans, String>() {
            @Override
            public String getKey(Spans spans) throws Exception {
//                return null;
                return (spans.parent.trim()+";"+spans.api.trim()).trim();
            }
        }).timeWindow(Time.milliseconds(60*1000)).process(new GraphAggProcessWindowFunction());


//
//        out1.print();
//        DataStream<Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long>> out1 =
        env.execute();
    }
//1633607526581
//1633607520000, end=1633607550000
//1633607820000
//1633607850000
//    1633607730000, end=1633607760000
//    public static class TraceProcessWinFunction extends ProcessWindowFunction<>
}
