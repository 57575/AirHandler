package AirHandler.Functions.sink;

import AirHandler.models.outputs.StrategyAbnormalRecord;
import AirHandler.utils.PostDataTable;
import AirHandler.utils.PostMessage;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;

public class TableSinkFunction extends RichSinkFunction<StrategyAbnormalRecord> {


    private final Logger log;
    private final String tableHost;
    private final String tableName;
    private final String projectId;
    private final String warningHost;
    private final String receiverJsonStr;
    private final String templateId;

    public TableSinkFunction(String tableHost, String table, String projectId, String warningHost, String receiverJsonStr, String templateId, Logger log) {
        this.log = log;
        this.tableHost = tableHost;
        this.tableName = table;
        this.projectId = projectId;
        this.receiverJsonStr = receiverJsonStr;
        this.templateId = templateId;
        this.warningHost = warningHost;
    }

    @Override
    public void invoke(StrategyAbnormalRecord item, Context context) throws Exception {
        if (item.IsFinish) {
            PostDataTable.AddTableData(tableHost, projectId, "cube", tableName, item.toCubeJsonStr(), log);
        } else {
            PostMessage.PostWarningMessage(warningHost, projectId, templateId, receiverJsonStr, item, log);
        }
    }

}

