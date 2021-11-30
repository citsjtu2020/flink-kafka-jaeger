import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import connectors.influxdb2.sink.writer.InfluxDBSchemaSerializer;
import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;

public class QPSInfluxSerializer implements InfluxDBSchemaSerializer<QPSs> {
    @Override
    public Point serialize(QPSs element, SinkWriter.Context context) throws IOException {
//        return null;
        final Point dataPoint = new Point("qpsofapi");
//            public QPSs(String api,double meandur,double mindur,double maxdur,double std,double p50dur,double p95dur,double p99dur,long count,long timestamp){
        dataPoint.addTag("api", String.valueOf(element.getApi().trim()));
        dataPoint.addField("mean", element.getMeandur());
        dataPoint.addField("min",element.getMindur());
        dataPoint.addField("max",element.getMaxdur());
        dataPoint.addField("std",element.getStd());
        dataPoint.addField("p50",element.getP50dur());
        dataPoint.addField("p95",element.getP95dur());
        dataPoint.addField("p99",element.getP99dur());
        dataPoint.addField("count",element.getCount());
//        dataPoint
        dataPoint.time(element.getTimestamp(), WritePrecision.MS);
        return dataPoint;
    }
}
