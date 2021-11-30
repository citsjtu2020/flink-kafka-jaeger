package connectors.mongo.config;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
@Getter
@Setter
public class SinkConfiguration implements Serializable {
    //是否是事务
    private boolean isTransactional;
    //在检查是点上刷新
    private boolean isFlushOnCheckpoint;
    //桶大小
    private long bulkFlushSize;
    //桶刷写数据库间隔
    private long bulkFlushInterval;
}