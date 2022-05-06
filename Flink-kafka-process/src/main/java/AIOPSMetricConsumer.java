import connectors.influxdb2.sink.InfluxDBSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class AIOPSMetricConsumer {
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
         // 20: influx data sink organization
        if (extract_args.containsKey("influx.measure".trim())){
            paramConfig.setInflux_measure(extract_args.get("influx.measure"));
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        {"log_id":"zIy5pX8B2wYFm91x6xZf","timestamp":"1650517696","cmdb_id":"frontend-0",
//        "log_name":"log_frontend-envoy_gateway",
//        "value":"\"POST /hipstershop.CartService/GetCart HTTP/2\" 200 - via_upstream - \"-\" 43 5 1 1 \"-\" \"grpc-go/1.31.0\" \"81cce4ca-f657-9b07-bf7a-c07f0c2d9d55\" \"cartservice:7070\" \"172.20.10.166:7070\" outbound|7070||cartservice.ts.svc.cluster.local 172.20.10.138:59988 10.68.145.247:7070 172.20.10.138:37660 - default"}
        InfluxDBSink<Metrics> influxDBSink = InfluxDBSink.builder()
        .setInfluxDBSchemaSerializer(new MetricsInfluxSerializer(paramConfig.getInflux_measure()))
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
//        mongoproperties.setProperty(connectors.mongo.config.MongoOptions.SINK_TRANSACTION_ENABLED, paramConfig.getMongo_trans_enable());
//        mongoproperties.setProperty(connectors.mongo.config.MongoOptions.SINK_FLUSH_ON_CHECKPOINT, paramConfig.getMongo_flush_checkpoint());
//        mongoproperties.setProperty(connectors.mongo.config.MongoOptions.SINK_FLUSH_SIZE, String.valueOf(paramConfig.getMongo_flush_size()));
//        mongoproperties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(paramConfig.getMongo_flush_interval()));
//        DataStreamSource<Spans> dataStreamSource = env.addSource(new MySpanSource() );
//        connectors.mongo.sink.MongoSink<Spans> mongoSink = new MongoSink<>("mongodb://"+paramConfig.getMongo_user()+":"+paramConfig.getMongo_pwd()+"@"+paramConfig.getMongo_ip()+":"+paramConfig.getMongo_port(), paramConfig.getMongo_database(), paramConfig.getMongo_collection(),
//                               new SpansDocumentSerializer(), mongoproperties);
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

        DataStream<Metrics> sourcecsv =
        streamSource.process(new ProcessFunction<ObjectNode,Metrics>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<Metrics> out) throws Exception {
//{"timestamp":"1650517680","cmdb_id":"adservice.ts:8088","kpi_name":"jvm_memory_MB_init.heap","value":"10.0"}
               if (value.get("value").has("kpi_name")){
                    String name = value.get("value").get("kpi_name").asText().trim();

                String pod = value.get("value").get("cmdb_id").asText().trim();
//                component
                long starttime = value.get("value").get("timestamp").asLong();
//
                double valuevar = value.get("value").get("value").asDouble();
                Metrics logout = new Metrics();
                logout.setApi(pod);
                logout.setTimestamp(starttime);
                logout.setName(name);
                logout.setValue(valuevar);
//                logout.setValue();
                out.collect(logout);
               }

            }
        }
        );
        sourcecsv.sinkTo(influxDBSink);
//
//        out1.print();
//        DataStream<Tuple10<String,Double,Double,Double,Double,Double,Double,Double,Long,Long>> out1 =
        env.execute();
    }
}
