package AirHandler.operators;

import AirHandler.Functions.Redises.AirHandlerSourceRedisMapFunction;
import AirHandler.Functions.Redises.RedisSourceFunction;
import AirHandler.Functions.sink.RedisSinkFunction;
import AirHandler.Functions.sink.TableSinkFunction;
import AirHandler.models.AirHandlerCubeItem;
import AirHandler.models.ExternalTask;
import AirHandler.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;


public class TemperatureAndSeasonOperator {

    private static String taskId;
    private static String operatorName;
    //数据源参数
    private static String redisUrl;
    private static String redisPassword;
    private static int redisDb;
    private static String airHandlerRedisChannelKey;

    private static int projectId;

    //异常记录cube host
    private static String cubeHost;
    //异常记录cube的id
    private static String cubeId;
    private static List<String> sensorKeys;

    //季节、温度参数
    private static List<Integer> summer = new ArrayList<>(Arrays.asList(5, 6, 7, 8, 9, 10));
    private static List<Integer> winter = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 11, 12));
    private static double summerTem = 26;
    private static double winterTem = 20;

    //报警参数
    private static String receiverJsonStr;
    private static String templateId;
    private static String warningHost;

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
        DataStream<AirHandlerCubeItem> waitCalculateStream = env
                .addSource(new RedisSourceFunction(redisUrl, redisPassword, redisDb, airHandlerRedisChannelKey, projectId, logger))
                .name(taskId + "airHandler-strategy-abnormal")
                .flatMap(new AirHandlerSourceRedisMapFunction(logger, sensorKeys));

        //计算
        DataStream<StrategyAbnormalRecord> recordDataStream =
                waitCalculateStream.flatMap(new TemperatureAndSeasonCalculator(summer, winter, summerTem, winterTem, operatorName));
        //报警
        recordDataStream.addSink(new RedisSinkFunction(redisUrl, redisPassword, redisDb, cubeId, projectId, logger));
        recordDataStream.addSink(new TableSinkFunction(cubeHost, cubeId, String.valueOf(projectId), warningHost, receiverJsonStr, templateId, logger));

        try {
            env.execute("AirHandler-analysis-" + parameter.TaskId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void GetParameters(ExternalTask parameters) throws Exception {
        try {
            taskId = parameters.TaskId;
            redisUrl = System.getenv("REDIS");
            redisPassword = System.getenv("REDIS_PASSWORD");
            redisDb = parameters.OperatorParameter.getInteger("RedisDb");
            airHandlerRedisChannelKey = parameters.OperatorParameter.getString("AirHandlerCubeId");
            projectId = parameters.ProjectId;
            cubeHost = System.getenv("WEB_URL");
            cubeId = parameters.OperatorParameter.getString("CubeId");

            sensorKeys = parameters.AirHandlerList.toJavaList(String.class);
            operatorName = parameters.Operator.name();
            receiverJsonStr = parameters.Warning.getJSONArray("Receivers").toJSONString();
            templateId = parameters.Warning.getString("TemplateId");
            warningHost = parameters.Warning.getString("Host");

            summer = parameters.OperatorParameter.getJSONArray("SummerMonth").toJavaList(int.class);
            winter = parameters.OperatorParameter.getJSONArray("WinterMonth").toJavaList(int.class);
            summerTem = parameters.OperatorParameter.getDouble("SummerTem");
            winterTem = parameters.OperatorParameter.getDouble("WinterTem");
        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }
}
