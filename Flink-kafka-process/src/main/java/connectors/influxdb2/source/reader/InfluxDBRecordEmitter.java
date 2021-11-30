package connectors.influxdb2.source.reader;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import connectors.influxdb2.source.reader.deserializer.InfluxDBDataPointDeserializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import connectors.influxdb2.common.DataPoint;
import connectors.influxdb2.source.split.InfluxDBSplit;

@Internal
public final class InfluxDBRecordEmitter<T> implements RecordEmitter<DataPoint, T, InfluxDBSplit> {
     private final InfluxDBDataPointDeserializer<T> dataPointDeserializer;
     public InfluxDBRecordEmitter(final InfluxDBDataPointDeserializer<T> dataPointDeserializer) {
        this.dataPointDeserializer = dataPointDeserializer;
    }
    @Override
    public void emitRecord(DataPoint dataPoint, SourceOutput<T> sourceOutput, InfluxDBSplit influxDBSplit) throws Exception {
        sourceOutput.collect(this.dataPointDeserializer.deserialize(dataPoint), dataPoint.getTimestamp());
    }
}
