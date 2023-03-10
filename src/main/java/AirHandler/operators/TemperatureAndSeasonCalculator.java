package AirHandler.operators;

import AirHandler.enums.AirHandlerMode;
import AirHandler.models.AirHandlerCubeItem;
import AirHandler.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class TemperatureAndSeasonCalculator extends RichFlatMapFunction<AirHandlerCubeItem, StrategyAbnormalRecord> implements CheckpointedFunction {
    private static final long serialVersionUID = -7348932276816657783L;
    List<Integer> Summer;
    List<Integer> Winter;
    double SummerTem = 26;
    double WinterTem = 20;
    String OperatorName;
    String taskId;
    private transient ListState<StrategyAbnormalRecord> unfinishedRecordState;
    Map<String, StrategyAbnormalRecord> unfinishedRecords;

    public TemperatureAndSeasonCalculator(String taskId, List<Integer> summer, List<Integer> winter, double summerTem, double winterTem, String operatorName) {
        this.taskId = taskId;
        this.Summer = summer;
        this.Winter = winter;
        this.SummerTem = summerTem;
        this.WinterTem = winterTem;
        this.OperatorName = operatorName;
        this.unfinishedRecords = new HashMap<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        unfinishedRecordState.clear();
        for (StrategyAbnormalRecord record : unfinishedRecords.values()) {
            unfinishedRecordState.add(record);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<StrategyAbnormalRecord> occMapState
                = new ListStateDescriptor<StrategyAbnormalRecord>(
                taskId + "unfinishedRecords",
                TypeInformation.of(new TypeHint<StrategyAbnormalRecord>() {
                })
        );
        unfinishedRecordState = context.getOperatorStateStore().getListState(occMapState);

        if (context.isRestored()) {
            for (StrategyAbnormalRecord record : unfinishedRecordState.get()) {
                unfinishedRecords.put(record.SensorKey, record);
            }
        }
    }

    @Override
    public void flatMap(AirHandlerCubeItem item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        //?????????????????????
        boolean waitClose = unfinishedRecords.containsKey(item.DeviceKey);
        //???????????????????????????
        double measureTem = SummerTem;
        boolean warning = false;
        //??????????????????????????????
        if (item.Status) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
            calendar.setTimeInMillis(item.TimeStamp);
            int month = calendar.get(Calendar.MONTH) + 1;
            //??????
            if (Summer.stream().anyMatch(x -> x == month)) {
                if (item.TemperatureSet < SummerTem && item.Mode.equals(AirHandlerMode.??????.toString())) {
                    warning = true;
                    measureTem = SummerTem;
                }
            }
            //??????
            else {
                if (item.TemperatureSet > WinterTem && item.Mode.equals(AirHandlerMode.??????.toString())) {
                    warning = true;
                    measureTem = WinterTem;
                }
            }
        }

        if (warning && (!waitClose)) {
            StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                    item.TimeStamp,
                    "??????????????????",
                    "????????????",
                    item.DeviceKey,
                    this.OperatorName,
                    ""
            );
            record.SetMeasure("?????????" + measureTem + "???");
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
