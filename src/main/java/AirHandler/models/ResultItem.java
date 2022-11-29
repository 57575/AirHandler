package AirHandler.models;

import com.alibaba.fastjson.JSONObject;

public class ResultItem {
    public String Name;
    public long TimeStamp;
    public boolean IsWarning;
    /**
     * 应当包括
     * <p>status                开关,bool;</p>
     * <p>temperature_set       设置温度,double;</p>
     * <p>temperature           实际温度,double;</p>
     * <p>mode                  模式,string</p>
     */
    public JSONObject OriginalData;

    public String Message;

    public ResultItem() {
        Name = "";
        TimeStamp = 0;
        IsWarning = false;
        OriginalData = new JSONObject();
        Message = "";
    }

    public void SetStatus(boolean value) {
        OriginalData.put("status", value);
    }

    public void SetTemperatureSet(double value) {
        OriginalData.put("temperature_set", value);
    }

    public void SetTemperature(double value) {
        OriginalData.put("temperature", value);
    }

    public void SetMode(String value) {
        OriginalData.put("mode", value);
    }

    public void SetName(String name) {
        if (Name == null || Name.isEmpty()) {
            Name = name;
        }
    }

    public void SetTimeStamp(long timeStamp) {
        if (timeStamp > TimeStamp) {
            TimeStamp = timeStamp;
        }
    }

    public void SetIsWarning(boolean value) {
        IsWarning = value;
    }

    public void AppendMessage(String m) {
        Message.concat("|").concat(m);
    }


}
