package AirHandler.models;

import com.alibaba.fastjson.JSONObject;
import AirHandler.enums.Operators;

import java.util.List;

/**
 * datapipeline中的ExternalTask parametersJson参数
 */
public class ExternalTask {
    public String TaskId;
    public String KafkaServer;
    public String Source;
    public long TimeStamp;
    public Operators Operator;
    public JSONObject OperatorParameter;
    public JSONObject Warning;
}
