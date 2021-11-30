import connectors.influxdb2.sink.InfluxDBSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class influx_test {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        InfluxDBSink<QPSs> influxDBSink = InfluxDBSink.builder()
        .setInfluxDBSchemaSerializer(new QPSInfluxSerializer())
        .setInfluxDBUrl("http://192.168.1.160:8086")           // http://localhost:8086
        .setInfluxDBUsername("k8s") // admin
//                CREATE USER k8s with PASSWORD 'k8s123'
        .setInfluxDBPassword("k8s123") // admin
        .setInfluxDBBucket("test")     // default
                .setWriteBufferSize(20)
        .setInfluxDBOrganization("influxdata")  // influxdata
                .setInfluxDBVersion(1)
        .build();

        DataStreamSource<QPSs> dataStreamSource = env.addSource(new MyQPSSource());
        dataStreamSource.sinkTo(influxDBSink);

        env.execute();
    }
}
