import connectors.influxdb2.sink.InfluxDBSink;
import connectors.mongo2.config.MongoOptions;
import connectors.mongo2.sink.MongoSink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import io.jaegertracing.api_v2.Model;
import model.ProtoUnmarshaler;
import model.Spanraw;
import model.Traceraw;

import com.typesafe.sslconfig.util.PrintlnLogger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) throws Exception {
//        Flink
        //备选方案：最熟悉的就是直接解析成字符串，然后使用fastjson进行解析，速度也比较快，如果出现bug就改用简单的方法实现
//        String kafkaserver = args[0].trim();
//        String topic = args[1].trim();
        // args:
        // build a class ParamConfig for these parameters:
        HashMap<String,String> extract_args = new HashMap<>();
        for (String arg : args){
            String[] arg_split = arg.split("=",2);
            extract_args.put(arg_split[0].trim(),arg_split[1].trim());
        }
        ParamConfig paramConfig = new ParamConfig();
        // 0: kafka data source ip
        if (extract_args.containsKey("kafka.ip".trim())){
            paramConfig.setKafka_ip(extract_args.get("kafka.ip"));
        }
        // 1: kafka data source port
        if (extract_args.containsKey("kafka.port".trim())){
            paramConfig.setKafka_port(Integer.parseInt(extract_args.get("kafka.port")));
        }
        // 2: kafka data topic
         if (extract_args.containsKey("kafka.topic".trim())){
            paramConfig.setKafka_topic(extract_args.get("kafka.topic"));
        }
        // 3:influx data sink ip
        if (extract_args.containsKey("influx.ip".trim())){
            paramConfig.setInflux_ip(extract_args.get("influx.ip"));
        }
        // 4:influx data sink port
        if (extract_args.containsKey("influx.port".trim())){
            paramConfig.setInflux_port(Integer.parseInt(extract_args.get("influx.port")));
        }
        // 5: influx data sink user
        if (extract_args.containsKey("influx.user".trim())){
            paramConfig.setInflux_user(extract_args.get("influx.user"));
        }
        // 6: influx data sink pwd
        if (extract_args.containsKey("influx.pwd".trim())){
            paramConfig.setInflux_pwd(extract_args.get("influx.pwd"));
        }
        // 7: influx data sink database
        if (extract_args.containsKey("influx.database".trim())){
            paramConfig.setInflux_dadtabase(extract_args.get("influx.database"));
        }
        // 8: influx data sink organization
         if (extract_args.containsKey("influx.organization".trim())){
            paramConfig.setInflux_organization(extract_args.get("influx.organization"));
        }
        // 9: influx data buffer size
         if (extract_args.containsKey("influx.buffer".trim())){
            paramConfig.setInflux_buffer(Integer.parseInt(extract_args.get("influx.buffer")));
        }
        // 10: mongo TRANSACTION_ENABLED
        if (extract_args.containsKey("mongo.trans.enable".trim())){
            paramConfig.setMongo_trans_enable(extract_args.get("mongo.trans.enable"));
        }
        // 11: mongo FLUSH_ON_CHECKPOINT
        if (extract_args.containsKey("mongo.flush.checkpoint".trim())){
            paramConfig.setMongo_flush_checkpoint(extract_args.get("mongo.flush.checkpoint"));
        }
        // 12: mongo FLUSH_SIZE
        if (extract_args.containsKey("mongo.flush.size".trim())){
            paramConfig.setMongo_flush_size(Long.parseLong(extract_args.get("mongo.flush.size")));
        }
        // 13: mongo FLUSH_INTERVAL
        if (extract_args.containsKey("mongo.flush.interval".trim())){
            paramConfig.setMongo_flush_interval(Long.parseLong(extract_args.get("mongo.flush.interval")));
        }
        // 14: mongo user
        if (extract_args.containsKey("mongo.user".trim())){
            paramConfig.setMongo_user(extract_args.get("mongo.user"));
        }
        // 15: mongo pwd
        if (extract_args.containsKey("mongo.pwd".trim())){
            paramConfig.setMongo_pwd(extract_args.get("mongo.pwd"));
        }
        // 16: mongo ip
        if (extract_args.containsKey("mongo.ip".trim())){
            paramConfig.setMongo_ip(extract_args.get("mongo.ip"));
        }
        // 17: mongo port
         if (extract_args.containsKey("mongo.port".trim())){
            paramConfig.setMongo_port(Integer.parseInt(extract_args.get("mongo.port")));
        }
        // 18: mongo database
        if (extract_args.containsKey("mongo.database".trim())){
            paramConfig.setMongo_database(extract_args.get("mongo.database"));
        }
        // 19: mongo collection
         if (extract_args.containsKey("mongo.collection".trim())){
            paramConfig.setMongo_collection(extract_args.get("mongo.collection"));
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        InfluxDBSink<QPSs> influxDBSink = InfluxDBSink.builder()
        .setInfluxDBSchemaSerializer(new QPSInfluxSerializer())
        .setInfluxDBUrl("http://"+paramConfig.getInflux_ip()+":"+paramConfig.getInflux_port())           // http://localhost:8086
        .setInfluxDBUsername(paramConfig.getInflux_user()) // admin
//                CREATE USER k8s with PASSWORD 'k8s123'
        .setInfluxDBPassword(paramConfig.getInflux_pwd()) // admin
        .setInfluxDBBucket(paramConfig.getInflux_dadtabase())     // default
                .setWriteBufferSize(paramConfig.getInflux_buffer())
        .setInfluxDBOrganization(paramConfig.getInflux_organization())  // influxdata
                .setInfluxDBVersion(1)
        .build();

        Properties mongoproperties = new Properties();
        mongoproperties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, paramConfig.getMongo_trans_enable());
        mongoproperties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, paramConfig.getMongo_flush_checkpoint());
        mongoproperties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(paramConfig.getMongo_flush_size()));
        mongoproperties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(paramConfig.getMongo_flush_interval()));
//        DataStreamSource<Spans> dataStreamSource = env.addSource(new MySpanSource() );
//        Spans
//        SpansDocumentSerializer()
        MongoSink<AIOPSGraphSpan> mongoSink = new MongoSink<>("mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port(), paramConfig.getMongo_database(), "service_span",
        new AIOPSGraphSpanDocumentSerializer(), mongoproperties);
//        AIOPSGraphSpanDocumentSerializer()
//        dataStreamSource.sinkTo();
////`mongodb://[username:password@]host1[:port1][,host2[:port2],…[,hostN[:portN]]][/[database][?options]]
        Properties properties = new Properties();
//        "10.100.233.199:9092"
//        properties.setProperty("bootstrap.servers",kafkaserver);
//        +":"+paramConfig.getKafka_port()
        properties.setProperty("bootstrap.servers",paramConfig.getKafka_ip());
        System.out.println("start the consumer");
//        +":"+paramConfig.getKafka_port()
        System.out.println("bootstap_servers: " + paramConfig.getKafka_ip());
//        new JSONKeyValueDeserializationSchema(true)
//        ObjectNode
        FlinkKafkaConsumer<Spanraw> consumer = new FlinkKafkaConsumer<>(paramConfig.getKafka_topic(),new ProtoUnmarshaler(),properties);
        consumer.setStartFromGroupOffsets();
        DataStream<Spanraw> streamSource = env.addSource(consumer);
//        DataStream<ObjectNode> streamSource = env.addSource(new MySource());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple9<String,String,String,String,Long,Double,String,String,String>> sourcecsv =
        streamSource.process(new ProcessFunction<Spanraw, Tuple9<String,String,String,String,Long,Double,String,String,String>>() {
            @Override
            public void processElement(Spanraw value, Context ctx, Collector<Tuple9<String,String,String,String,Long,Double,String,String,String>> out) throws Exception {
//                System.out.println("start process");
//                value.get("value").get("traceID").asText().trim()
                String tid = value.getTraceId();
                String sid = value.getSpanId();
                double dur = value.getDuration();
//                JsonNode process = value.get("value").get("process");
                String service = value.getServiceName();
//                String service = process.get("serviceName").asText().trim();
                String pod = "";
                String endpoint = "";
                String protocol = "";

                if (value.getTags().size() > 0){
//                    ListIterator<>
                    if (value.containTagItem("hostname")){
                        pod = value.getTagsItem("hostname");
                    }
                    endpoint = value.getTagsItem("ip");
//                    "component"
                    protocol = value.getTagsItem("component");
                }
//                System.out.println("init success in processElement");
//                component
//                if (process.has("tags")) {
//                    JsonNode tags = process.get("tags");
//                    for (Iterator<JsonNode> elements = tags.elements(); elements.hasNext();){
//                        JsonNodpe next = elements.next();
//                        String aim = next.get("key").asText().trim();
//                        if (aim.trim().equals("hostname")){
////                    System.out.println(jtag);
//                            pod = next.get("value").asText();
////                    break;
//                        }
//                        if (aim.trim().equals("ip")){
//                            endpoint = next.get("value").asText();
//                        }
//                        if ((!endpoint.isEmpty()) && (!pod.isEmpty())){
//                            break;
//                        }
//                    }
//                }
//                JsonNode spantags = value.get("value").get("tags");
//                for(Iterator<JsonNode> elements = spantags.elements();elements.hasNext();){
//                    JsonNode next = elements.next();
//                    String aim = next.get("key").asText().trim();
//                    if (aim.trim().equals("component")){
////                System.out.println(jtag);
//                        protocol = next.get("value").asText().trim();
//                        break;
//                    }
//                }
//                get("value").get("startTimeMillis").asLong()
                long starttime = value.getStartTimeMillis();
//                get("value").get("operationName").asText().trim()
                String operation = value.getOperationName();
                String refer = "0";
                boolean childs = false;
                String outapi = "";
                if (!value.getParentId().equals("")){
                    childs = true;
                    refer = value.getParentId();
                }else{
                    refer = "client";
                }
//                object.getJSONArray("references").isEmpty()== false
//                if (value.get("value").get("references").isEmpty() == false){
////                    value.get("value").get("references").
//                    refer = value.get("value").get("references").get(0).get("spanID").asText().trim();
//                    childs = true;
//                }
              if(!(service.equals("simple-streaming.jaeger")) && !(service.contains("jaeger"))&& !(service.contains("simple-streaming"))){
                    operation = operation.replace('/','-');
                    if (operation.charAt(0) != '-'){
                        operation = "-"+operation.trim();
                    }
//                    if (!childs){
//                        outapi = "http".trim();
//                    }else{
//                        outapi = service.trim()+operation;
//                    }
                  outapi = service.trim()+operation;
//                System.out.println("trace: "+tid);
//                System.out.println("span: "+sid);
//                System.out.println("duration: "+dur);
//                service,operation
                    out.collect(new Tuple9<>(tid,sid,refer,outapi,starttime,dur,pod,endpoint,protocol));

                }

            }
        }

        );
        SingleOutputStreamOperator<Tuple9<String, String, String, String, Long, Double, String, String, String>> source1 =  sourcecsv.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple9<String, String, String, String, Long, Double, String, String, String>>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Tuple9<String, String, String, String, Long, Double, String, String, String> trace) {
                return trace.f4;
            }
        });
//Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long>
        DataStream<QPSs> out1 = source1
                .keyBy(t->t.f3)
                .timeWindow(Time.milliseconds(30*1000)).process(new TraceProcessWindowFunction());


        DataStream<Spans> out2 = source1.keyBy(t->t.f0).timeWindow(Time.milliseconds(30*1000)).process(new GraphProcessWindowFunction());
        DataStream<AIOPSGraphSpan> out3 = out2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Spans>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Spans spans) {
                return spans.timestamp;
            }
        }).keyBy(new KeySelector<Spans, String>() {
            @Override
            public String getKey(Spans spans) throws Exception {
//                return null;
//                return (spans.parent.trim()+";"+spans.api.trim()).trim();
                return (spans.parent.trim()+";"+spans.api.trim()).trim()+";"+spans.parent_type.trim();
            }
        }).timeWindow(Time.milliseconds(60*1000)).process(new GraphAggProcessWindowFunction());
        out1.sinkTo(influxDBSink);
        out3.sinkTo(mongoSink);

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
