import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import connectors.influxdb2.sink.writer.InfluxDBSchemaSerializer;
import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;

public class MetricsInfluxSerializer implements InfluxDBSchemaSerializer<Metrics> {
    public String measurements = "pod";
    public MetricsInfluxSerializer(String measurements){
        this.measurements = measurements;
    }

    public String getMeasurements() {
        return this.measurements;
    }

    public void setMeasurements(String measurements) {
        this.measurements = measurements;
    }

    @Override
    public Point serialize(Metrics element, SinkWriter.Context context) throws IOException {
//        final Point dataPoint = new Point");
        final Point dataPoint = new Point(this.measurements);
        dataPoint.addTag("cmdb", String.valueOf(element.getApi().trim()));
//        dataPoint.addTag那麼e", String.valueOf(element.getApi().trim()));
        dataPoint.addTag("name",String.valueOf(element.getName().trim()));
        dataPoint.addField("value", element.getValue());
//        dataPoint
        dataPoint.time(element.getTimestamp()*1e3, WritePrecision.MS);
        return dataPoint;
    }
}
