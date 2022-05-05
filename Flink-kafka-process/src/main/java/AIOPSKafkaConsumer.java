import akka.remote.WireFormats;
import connectors.influxdb2.sink.InfluxDBSink;
import connectors.mongo2.config.MongoOptions;
import connectors.mongo2.sink.MongoSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AIOPSKafkaConsumer {
     public static void main(String[] args) throws Exception {

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
//         --mongo.flush.size=20
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
        // 19: mongo collection for qps
         if (extract_args.containsKey("mongo.collection.span".trim())){
            paramConfig.setMongo_collection_span(extract_args.get("mongo.collection.span"));
        }
         //20 mongo collection for rt
          if (extract_args.containsKey("mongo.collection.service".trim())){
            paramConfig.setMongo_collection_service(extract_args.get("mongo.collection.service"));
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(paramConfig);
         Properties mongoproperties = new Properties();
         mongoproperties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, paramConfig.getMongo_trans_enable());
         mongoproperties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, paramConfig.getMongo_flush_checkpoint());
         mongoproperties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(paramConfig.getMongo_flush_size()));
         mongoproperties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(paramConfig.getMongo_flush_interval()));
//        DataStreamSource<Spans> dataStreamSource = env.addSource(new MySpanSource() );
         String span_svc = paramConfig.getMongo_collection_span()+"_svc";
         String service_svc = paramConfig.getMongo_collection_service()+"_svc";

         String span_pod = paramConfig.getMongo_collection_span()+"_pod";
         String service_pod = paramConfig.getMongo_collection_service()+"_pod";
//"mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port()

//Properties properties = new Properties();
//    properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
//    properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
//    properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
//    properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));
//
//    env.addSource(...)
//       .sinkTo(new MongoSink<>("mongodb://user:password@127.0.0.1:27017", "mydb", "mycollection",
//                               new StringDocumentSerializer(), properties));
         MongoSink<AIOPSGraphSpan> mongoSink_span_svc = new MongoSink<>("mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port(), paramConfig.getMongo_database(), span_svc,
                               new AIOPSGraphSpanDocumentSerializer(), mongoproperties);
         MongoSink<QPSs> mongoSink_service_svc = new MongoSink<>("mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port(), paramConfig.getMongo_database(), service_svc,
                               new QPSsDocumentSerializer(), mongoproperties);
         MongoSink<AIOPSGraphSpan> mongoSink_span_pod = new MongoSink<>("mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port(), paramConfig.getMongo_database(), span_pod,
                               new AIOPSGraphSpanDocumentSerializer(), mongoproperties);
         MongoSink<QPSs> mongoSink_service_pod = new MongoSink<>("mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port(), paramConfig.getMongo_database(), service_pod,
                               new QPSsDocumentSerializer(), mongoproperties);
//        dataStreamSource.sinkTo();
////`mongodb://[username:password@]host1[:port1][,host2[:port2],…[,hostN[:portN]]][/[database][?options]]
        Properties properties = new Properties();
//        "10.100.233.199:9092"
//        properties.setProperty("bootstrap.servers",kafkaserver);
        properties.setProperty("bootstrap.servers",paramConfig.getKafka_ip()+":"+paramConfig.getKafka_port());

        System.out.println("start the consumer");

        FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>(paramConfig.getKafka_topic(),new JSONKeyValueDeserializationSchema(true),properties);
        consumer.setStartFromGroupOffsets();
        DataStream<ObjectNode> streamSource = env.addSource(consumer);
//        DataStream<ObjectNode> streamSource = env.addSource(new MySource());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//  3,org.apache.flink.api.common.time.Time.of(5,TimeUnit.MINUTES),org.apache.flink.api.common.time.Time.of(10,TimeUnit.SECONDS)
//        ));


        //        {"timestamp":"1650517025591","cmdb_id":"frontend-0",
//        "span_id":"e75db2d334118d6b",
//        "trace_id":"8915ad35a681682672dd003ccd6b5af2",
//        "duration":"2244","type":"rpc","status_code":"0",
//        "operation_name":"hipstershop.CurrencyService/Convert",
//        "parent_span":"1ae5793e9e0bbf71"}

        DataStream<Tuple9<String,String,String,String,Long,Long,String,String,String>> sourcecsv =
                streamSource.process(new ProcessFunction<ObjectNode, Tuple9<String,String,String,String,Long,Long,String,String,String>>() {
                    @Override
                    public void processElement(ObjectNode value, Context ctx, Collector<Tuple9<String,String,String,String,Long,Long,String,String,String>> out) throws Exception {
//                        System.out.println(value);
                        if (value.get("value").has("span_id")){
                            String tid = value.get("value").get("trace_id").asText().trim();
                            String sid = value.get("value").get("span_id").asText().trim();
                            long dur = value.get("value").get("duration").asLong();
//                          JsonNode process = value.get("value").get("process");
                            String pod = value.get("value").get("cmdb_id").asText().trim();
                            String service = pod.split("-")[0].trim();
//                          String endpoint = "";
                            String protocol = value.get("value").get("type").asText().trim();
//                component

                            long starttime = value.get("value").get("timestamp").asLong();
                            String operation = value.get("value").get("operation_name").asText().trim();
                            String refer = "0";
                            boolean childs = false;
                            String outapi = "";
//                          "parent_span"
                            if (value.get("value").has("parent_span")){
                                String tmps = value.get("value").get("parent_span").asText().trim();
                                if (tmps==null||tmps.length()<= 0){
                                    refer = "client";
                                    childs = false;
                                }else{
                                    refer = tmps;
                                    childs = true;
                                }
                            }
                            //                        operation = operation.replace('/','-');
                            if (operation.charAt(0) != '-'){
                                operation = "-"+operation.trim();
                            }
                            outapi = service.trim()+operation;
                            String pod_out_api = pod.trim()+operation;
                            out.collect(new Tuple9<>(tid,sid,refer,outapi,starttime,dur,pod,pod_out_api,protocol));
                        }

                    }
                }
        );
        SingleOutputStreamOperator<Tuple9<String, String, String, String, Long, Long, String,String,String>> source1 =  sourcecsv.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple9<String, String, String, String, Long, Long, String,String,String>>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Tuple9<String, String, String, String, Long, Long, String,String , String> trace) {
                return trace.f4;
            }
        });
//Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long>
        DataStream<QPSs> out1 = source1
                .keyBy(t->t.f3)
                .timeWindow(Time.milliseconds(60*1000)).process(new AIOPSTraceProcessWindowFunction());

         DataStream<QPSs> podout1 = source1
                .keyBy(t->t.f7)
                .timeWindow(Time.milliseconds(60*1000)).process(new AIOPSPodTraceProcessWindowFunction());

        DataStream<AIOPSSpans> out2 = source1.keyBy(t->t.f0).timeWindow(Time.milliseconds(60*1000)).process(new AIOPSGraphProcessWindowFunction());
//       阿他
        DataStream<AIOPSSpans> podout2 = source1.keyBy(t->t.f0).timeWindow(Time.milliseconds(60*1000)).process(new AIOPSGraphProcessWindowFunction(true));
        DataStream<AIOPSGraphSpan> out3 = out2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AIOPSSpans>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(AIOPSSpans spans) {
                return spans.timestamp;
            }
        }).keyBy(new KeySelector<AIOPSSpans, String>() {
            @Override
            public String getKey(AIOPSSpans spans) throws Exception {
//                return null;
                return (spans.parent.trim()+";"+spans.api.trim()).trim()+";"+spans.parent_type.trim();
            }
        }).timeWindow(Time.milliseconds(60*1000)).process(new AIOPSGraphAggProcessWindowFunction());

        DataStream<AIOPSGraphSpan> podout3 = podout2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AIOPSSpans>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(AIOPSSpans spans) {
                return spans.timestamp;
            }
        }).keyBy(new KeySelector<AIOPSSpans, String>() {
            @Override
            public String getKey(AIOPSSpans spans) throws Exception {
//                return null;
                return (spans.parent.trim()+";"+spans.api.trim()).trim()+";"+spans.parent_type.trim();
            }
        }).timeWindow(Time.milliseconds(60*1000)).process(new AIOPSGraphAggProcessWindowFunction());

        out1.sinkTo(mongoSink_service_svc);
        out3.sinkTo(mongoSink_span_svc);
        podout1.sinkTo(mongoSink_service_pod);
        podout3.sinkTo(mongoSink_span_pod);
        env.execute();
    }
}