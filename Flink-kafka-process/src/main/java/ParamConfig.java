public class ParamConfig {
    // build a class ParamConfig for these parameters:
        // 0: kafka data source ip
    private String kafka_ip;
        // 1: kafka data source port
    private int kafka_port;
        // 2: kafka data topic
    private String kafka_topic;
        // 3:influx data sink ip
    private String influx_ip;
        // 4:influx data sink port
    private int influx_port;
        // 5: influx data sink user
    private String influx_user;
        // 6: influx data sink pwd
    private String influx_pwd;
        // 7: influx data sink database
    private String influx_dadtabase;
        // 8: influx data sink organization
    private String influx_organization;
        // 9: influx data buffer size
    private int influx_buffer;
        // 10: mongo TRANSACTION_ENABLED
    private String mongo_trans_enable;
        // 11: mongo FLUSH_ON_CHECKPOINT
    private String mongo_flush_checkpoint;
        // 12: mongo FLUSH_SIZE
    private long mongo_flush_size;
    //  13: mongo_flush_interval
    private long mongo_flush_interval;
        // 14: mongo user
    private String mongo_user;
        // 15: mongo pwd
    private String mongo_pwd;
        // 16: mongo ip
    private String mongo_ip;
        // 17: mongo port
    private int mongo_port;
        // 18: mongo database
    private String mongo_database;
        // 19: mongo collection for spans
    private String mongo_collection_span;
        //20: mongo collection for service
    private String mongo_collection_service;
//
    private String influx_measure;

    public ParamConfig(){
        this.kafka_ip = "192.168.1.18";
        // 1: kafka data source port
        this.kafka_port = 9092;
        // 2: kafka data topic
//        --kafka.topic=jaeger-span
        this.kafka_topic = "jaeger-span";
        // 3:influx data sink ip
        this.influx_ip = "192.168.1.18";
        // 4:influx data sink port
        this.influx_port = 8086;
        // 5: influx data sink user
        this.influx_user = "admin";
        // 6: influx data sink pwd
        this.influx_pwd = "admin123";
        // 7: influx data sink database
        this.influx_dadtabase = "jaeger";
        // 8: influx data sink organization
        this.influx_organization = "influxdata";
        // 9: influx data buffer size
        this.influx_buffer = 20;
        // 10: mongo TRANSACTION_ENABLED
        this.mongo_trans_enable = "false";
        // 11: mongo FLUSH_ON_CHECKPOINT
        this.mongo_flush_checkpoint = "false";
        // 12: mongo FLUSH_SIZE
//        1_000
        this.mongo_flush_size = 10;
    //  13: mongo_flush_interval
//        10_000
        this.mongo_flush_interval = 30_000L;
        // 14: mongo user
        this.mongo_user = "jaeger";
        // 15: mongo pwd
        this.mongo_pwd = "jaeger123";
        // 16: mongo ip
        this.mongo_ip = "192.168.1.18";
        // 17: mongo port
        this.mongo_port = 27017;
        // 18: mongo database
        this.mongo_database = "jaeger";
        // 19: mongo collection
        this.mongo_collection_span = "span";
        this.mongo_collection_service = "service";
        this.influx_measure = "pod";
    }

    public int getInflux_port() {
        return this.influx_port;
    }

    public int getInflux_buffer() {
        return this.influx_buffer;
    }

    public int getKafka_port() {
        return this.kafka_port;
    }

    public long getMongo_flush_interval() {
        return this.mongo_flush_interval;
    }

    public int getMongo_port() {
        return this.mongo_port;
    }

    public long getMongo_flush_size() {
        return this.mongo_flush_size;
    }

    public String getInflux_dadtabase() {
        return this.influx_dadtabase;
    }

    public String getKafka_ip() {
        return this.kafka_ip;
    }

    public String getInflux_ip() {
        return this.influx_ip;
    }

    public String getInflux_organization() {
        return this.influx_organization;
    }

    public String getInflux_pwd() {
        return this.influx_pwd;
    }

    public String getInflux_user() {
        return this.influx_user;
    }

    public String getKafka_topic() {
        return this.kafka_topic;
    }

    public String getMongo_collection() {
        return this.mongo_collection_span;
    }

    public String getMongo_database() {
        return this.mongo_database;
    }

    public String getMongo_flush_checkpoint() {
        return this.mongo_flush_checkpoint;
    }

    public String getMongo_ip() {
        return this.mongo_ip;
    }

    public String getMongo_pwd() {
        return this.mongo_pwd;
    }

    public String getMongo_trans_enable() {
        return this.mongo_trans_enable;
    }

    public String getMongo_user() {
        return this.mongo_user;
    }

    public void setInflux_dadtabase(String influx_dadtabase) {
        this.influx_dadtabase = influx_dadtabase;
    }

    public void setInflux_buffer(int influx_buffer) {
        this.influx_buffer = influx_buffer;
    }

    public void setInflux_ip(String influx_ip) {
        this.influx_ip = influx_ip;
    }

    public void setInflux_organization(String influx_organization) {
        this.influx_organization = influx_organization;
    }

    public void setInflux_port(int influx_port) {
        this.influx_port = influx_port;
    }

    public void setInflux_pwd(String influx_pwd) {
        this.influx_pwd = influx_pwd;
    }

    public void setInflux_user(String influx_user) {
        this.influx_user = influx_user;
    }

    public void setKafka_ip(String kafka_ip) {
        this.kafka_ip = kafka_ip;
    }

    public void setKafka_port(int kafka_port) {
        this.kafka_port = kafka_port;
    }

    public void setKafka_topic(String kafka_topic) {
        this.kafka_topic = kafka_topic;
    }

    public void setMongo_collection(String mongo_collection) {
        this.mongo_collection_span = mongo_collection;
    }

    public void setMongo_database(String mongo_database) {
        this.mongo_database = mongo_database;
    }

    public void setMongo_flush_checkpoint(String mongo_flush_checkpoint) {
        this.mongo_flush_checkpoint = mongo_flush_checkpoint;
    }

    public void setMongo_flush_interval(long mongo_flush_interval) {
        this.mongo_flush_interval = mongo_flush_interval;
    }

    public void setMongo_flush_size(long mongo_flush_size) {
        this.mongo_flush_size = mongo_flush_size;
    }

    public void setMongo_ip(String mongo_ip) {
        this.mongo_ip = mongo_ip;
    }

    public void setMongo_port(int mongo_port) {
        this.mongo_port = mongo_port;
    }

    public void setMongo_pwd(String mongo_pwd) {
        this.mongo_pwd = mongo_pwd;
    }

    public void setMongo_trans_enable(String mongo_trans_enable) {
        this.mongo_trans_enable = mongo_trans_enable;
    }

    public void setMongo_user(String mongo_user) {
        this.mongo_user = mongo_user;
    }

    public void setMongo_collection_span(String mongo_collection_span) {
        this.mongo_collection_span = mongo_collection_span;
    }

    public void setMongo_collection_service(String mongo_collection_service) {
        this.mongo_collection_service = mongo_collection_service;
    }

    public String getMongo_collection_span() {
        return this.mongo_collection_span;
    }

    public String getMongo_collection_service() {
        return this.mongo_collection_service;
    }

    public String getInflux_measure() {
        return this.influx_measure;
    }

    public void setInflux_measure(String influx_measure) {
        this.influx_measure = influx_measure;
    }

    @Override
    public String toString() {
        return "ParamConfig{" +
                "kafka_ip='" + kafka_ip + '\'' +
                ", kafka_port=" + kafka_port +
                ", kafka_topic='" + kafka_topic + '\'' +
                ", influx_ip='" + influx_ip + '\'' +
                ", influx_port=" + influx_port +
                ", influx_user='" + influx_user + '\'' +
                ", influx_pwd='" + influx_pwd + '\'' +
                ", influx_dadtabase='" + influx_dadtabase + '\'' +
                ", influx_organization='" + influx_organization + '\'' +
                ", influx_buffer=" + influx_buffer +
                ", mongo_trans_enable='" + mongo_trans_enable + '\'' +
                ", mongo_flush_checkpoint='" + mongo_flush_checkpoint + '\'' +
                ", mongo_flush_size=" + mongo_flush_size +
                ", mongo_flush_interval=" + mongo_flush_interval +
                ", mongo_user='" + mongo_user + '\'' +
                ", mongo_pwd='" + mongo_pwd + '\'' +
                ", mongo_ip='" + mongo_ip + '\'' +
                ", mongo_port=" + mongo_port +
                ", mongo_database='" + mongo_database + '\'' +
                ", mongo_collection_span='" + mongo_collection_span + '\'' +
                ", mongo_collection_service='" + mongo_collection_service + '\'' +
                ", influx_measure='" + influx_measure + '\'' +
                '}';
    }
}
