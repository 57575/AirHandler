package AirHandler.operators;

import AirHandler.enums.AirHandlerMode;
import AirHandler.models.AirHandlerCubeItem;
import AirHandler.models.ResultItem;
import AirHandler.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class TemperatureAndSeasonCalculator implements FlatMapFunction<AirHandlerCubeItem, StrategyAbnormalRecord> {
    List<Integer> Summer;
    List<Integer> Winter;
    double SummerTem = 26;
    double WinterTem = 20;
    String OperatorName;
    HashMap<String, StrategyAbnormalRecord> unfinishedRecords;

    public TemperatureAndSeasonCalculator(List<Integer> summer, List<Integer> winter, double summerTem, double winterTem, String operatorName) {
        this.Summer = summer;
        this.Winter = winter;
        this.SummerTem = summerTem;
        this.WinterTem = winterTem;
        this.OperatorName = operatorName;
    }

    @Override
    public void flatMap(AirHandlerCubeItem item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        if (unfinishedRecords == null) {
            unfinishedRecords = new HashMap<>();
        }
        //尚未结束的事件
        boolean waitClose = unfinishedRecords.containsKey(item.DeviceKey);
        //该数据是否需要报警
        double measureTem = SummerTem;
        boolean warning = false;
        //关闭状态下，无需报警
        if (item.Status) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
            calendar.setTimeInMillis(item.TimeStamp);
            int month = calendar.get(Calendar.MONTH) + 1;
            //夏天
            if (Summer.stream().anyMatch(x -> x == month)) {
                if (item.TemperatureSet < SummerTem && item.Mode.equals(AirHandlerMode.制冷.toString())) {
                    warning = true;
                    measureTem = SummerTem;
                }
            }
            //冬天
            else {
                if (item.TemperatureSet > WinterTem && item.Mode.equals(AirHandlerMode.制热.toString())) {
                    warning = true;
                    measureTem = WinterTem;
                }
            }
        }

        if (warning && (!waitClose)) {
            StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                    item.TimeStamp,
                    "温度设定异常",
                    "暖通系统",
                    item.DeviceKey,
                    this.OperatorName,
                    ""
            );
            record.SetMeasure("调整到" + measureTem + "℃");
            record.SetOriginData(item.toJSONObject());
            unfinishedRecords.put(item.DeviceKey, record);
            collector.collect(record);
        }
        if (waitClose && (!warning)) {
            StrategyAbnormalRecord record = unfinishedRecords.get(item.DeviceKey);
            record.SetFinish(item.TimeStamp);
            record.SetCarbon(0, "kg");
            collector.collect(record);
            unfinishedRecords.remove(item.DeviceKey);
        }
    }

}
