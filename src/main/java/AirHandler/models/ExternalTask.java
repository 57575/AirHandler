package AirHandler.models;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import AirHandler.enums.Operators;

import java.util.List;

/**
 * datapipeline中的ExternalTask parametersJson参数
 */
public class ExternalTask {
    public String TaskId;
    public int ProjectId;
    public long TimeStamp;
    public Operators Operator;
    public JSONObject OperatorParameter;
    public JSONObject Warning;
    public JSONArray AirHandlerList;
    public String FlinkId;
    public String FlinkSecret;
}
