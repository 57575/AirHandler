package AirHandler.models;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

/**
 * 数据源cube结构
 */
public class CubeItem {
    @JSONField(name = "device_key")
    public String DeviceKey;
    @JSONField(name = "alarm_value")
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
    @JSONField(name = "status_value")
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
}
