package AirHandler.operators;

import AirHandler.Functions.CubeItemFilterInvalidFunction;
import AirHandler.Functions.SourceFunction;
import AirHandler.models.CubeItem;
import AirHandler.models.ExternalTask;
import AirHandler.models.ResultItem;
import AirHandler.utils.PostMessage;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;


public class TemperatureAndSeasonOperator {
    private static String kafkaServer;
    private static String sourceTopic;
    private static String sourceGroup;
    private static String sinkTopic;


    public static void Run(ExternalTask parameter) {

        //初始化参数
        try {
            GetParameters(parameter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Logger logger = LoggerFactory.getLogger("AirHandler-logs-" + parameter.TaskId);

        // 初始化流计算环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, org.apache.flink.api.common.time.Time.of(1, TimeUnit.MINUTES)));
        env.setParallelism(1);

        //获取源
        DataStream<JSONObject> sourceStream = SourceFunction.GetSource(env, kafkaServer, sourceTopic, sourceGroup, "time")
                .filter(new CubeItemFilterInvalidFunction(logger));

        //准备待计算源
        DataStream<CubeItem> waitCalculateStream = sourceStream
                .map(new MapFunction<JSONObject, CubeItem>() {
                    @Override
                    public CubeItem map(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("value").toJavaObject(CubeItem.class);
                    }
                });

        //计算
        DataStream<ResultItem> outputStream = waitCalculateStream
                .map(new MapFunction<CubeItem, ResultItem>() {
                    @Override
                    public ResultItem map(CubeItem item) throws Exception {
                        return TemperatureAndSeasonCalculator.Calculate(item);
                    }
                });

        //报警
        outputStream
                .map(new PostMessage(parameter.Warning, logger));

        Sink(outputStream);

        try {
            env.execute("AirHandler-analysis-" + parameter.TaskId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void GetParameters(ExternalTask parameters) throws Exception {
        try {
            kafkaServer = parameters.KafkaServer;
            sourceTopic = "withoutWindow-" + parameters.Source;
            sinkTopic = "AirHandler-analysis-" + parameters.TaskId;
            sourceGroup = "AirHandler-analysis-" + parameters.TaskId;
            TemperatureAndSeasonCalculator.SetSeason(
                    parameters.OperatorParameter.getJSONArray("SummerMonth").toJavaList(int.class),
                    parameters.OperatorParameter.getJSONArray("WinterMonth").toJavaList(int.class)
            );
            TemperatureAndSeasonCalculator.SetTemperature(
                    parameters.OperatorParameter.getDouble("SummerTem"),
                    parameters.OperatorParameter.getDouble("WinterTem")
            );

        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }


    private static void Sink(DataStream<ResultItem> dataStream) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaServer)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        dataStream.map(new MapFunction<ResultItem, String>() {
            @Override
            public String map(ResultItem windowedIllumination) throws Exception {
                return JSON.toJSONString(windowedIllumination, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue);
            }
        }).sinkTo(kafkaSink);

    }

}
