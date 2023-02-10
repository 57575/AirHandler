package AirHandler.models;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 数据源cube结构
 */
public class AirHandlerCubeItem {
    @JSONField(name = "device_key")
    public String DeviceKey;
    @JSONField(name = "alarm")
    public Boolean Alarm;
    /**
     * 设置温度
     */
    @JSONField(name = "temperature_set")
    public Double TemperatureSet;
    @JSONField(name = "temperature")
    public Double Temperature;
    /**
     * 开关状态
     */
    @JSONField(name = "status")
    public Boolean Status;
    /**
     * 模式设置
     */
    @JSONField(name = "mode")
    public String Mode;
    @JSONField(name = "speed_value")
    public int Speed;
    @JSONField(name = "time")
    public long TimeStamp;

    public Date Time;

    public void Import(JSONObject entity) {
        this.DeviceKey = entity.getString("device_key");
        String alarm = entity.getString("alarm_value");
        if (alarm.equals("false")) {
            this.Alarm = false;
        } else {
            this.Alarm = true;
        }
        this.TemperatureSet = entity.getDouble("temperature_set");
        this.Temperature = entity.getDouble("temperature");
        String status = entity.getString("status");
        if (status.equals("false")) {
            this.Status = false;
        } else {
            this.Status = true;
        }
        this.Mode = entity.getString("mode");
        this.Speed = entity.getInteger("speed_value");
        String timeStr = entity.getString("time");
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.Time = sdf.parse(timeStr);
            this.TimeStamp = this.Time.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public JSONObject toJSONObject() {
        return (JSONObject) com.alibaba.fastjson.JSONObject.toJSON(this);
    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }
}
