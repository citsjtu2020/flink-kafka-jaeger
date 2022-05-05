import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class AIOPSKafkaProducer {


    public static void main(String[] args) throws Exception {
//        String kafkaserver = args[0].trim();
//        String topic = args[1].trim();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //使用自定义的数据源
        DataStreamSource<String> dataStreamSource = env.addSource(new AIOPSSource());
        //配置bootstrap.servers的地址和端口
        Properties properties = new Properties();
        //配置bootstrap.servers的地址和端口
//        10.100.233.199
//        "10.100.233.199:9092"
//        System.out.println(kafkaserver);
//        System.out.println(topic);
        properties.setProperty("bootstrap.servers","192.168.1.18:9092");
        System.out.println("start the proceducer");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("jaeger-span",new SimpleStringSchema(),properties);
        producer.setWriteTimestampToKafka(true);
        dataStreamSource.addSink(producer);
        System.out.println("add the sink to the proceducer");
        env.execute();
    }

}
