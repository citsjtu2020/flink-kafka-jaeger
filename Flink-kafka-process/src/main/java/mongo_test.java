import connectors.mongo.config.MongoOptions;
import connectors.mongo.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class mongo_test {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //使用自定义的数据源
        Properties properties = new Properties();
        properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
        properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
        properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
        properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));
        DataStreamSource<Spans> dataStreamSource = env.addSource(new MySpanSource() );

        dataStreamSource.sinkTo(new MongoSink<>("mongodb://flinkadmin:flink@127.0.0.1:27017", "mydb", "mycollection",
                               new SpansDocumentSerializer(), properties));

        env.execute();

    }
}
