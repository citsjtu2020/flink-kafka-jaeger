import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import connectors.influxdb2.sink.writer.InfluxDBSchemaSerializer;
import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;

public class LOGSInfluxSerializer implements InfluxDBSchemaSerializer<LOGs> {
    @Override
    public Point serialize(LOGs element, SinkWriter.Context context) throws IOException {
//        return null;
        final Point dataPoint = new Point("logs");
        dataPoint.addTag("cmdb", String.valueOf(element.getApi().trim()));
//        dataPoint.addTag那麼e", String.valueOf(element.getApi().trim()));
        dataPoint.addTag("name",String.valueOf(element.getName().trim()));
        dataPoint.addField("value", element.getValue());

//        dataPoint
        dataPoint.time(element.getTimestamp()*1e3, WritePrecision.MS);
        return dataPoint;
    }
}
