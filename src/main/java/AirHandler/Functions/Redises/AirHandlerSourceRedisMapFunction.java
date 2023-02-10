package AirHandler.Functions.Redises;

import AirHandler.models.AirHandlerCubeItem;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

import java.util.List;

public class AirHandlerSourceRedisMapFunction implements FlatMapFunction<String, AirHandlerCubeItem> {

    private org.slf4j.Logger log;
    private List<String> keys;

    public AirHandlerSourceRedisMapFunction(Logger logger, List<String> keys) {
        this.log = logger;
        this.keys = keys;
    }

    @Override
    public void flatMap(String s, Collector<AirHandlerCubeItem> collector) throws Exception {
        JSONObject origin = com.alibaba.fastjson.JSON.parseObject(s);
        if (!origin.containsKey("item")) {
            System.out.println("空调redis数据错误;" + s);
            log.info("空调redis数据错误;" + s);
            return;
        }
        JSONObject item = origin.getJSONObject("item");
        if (!item.containsKey("value")) {
            System.out.println("空调redis数据不包含值;" + s);
            log.info("空调redis数据不包含值;" + s);
            return;
        }
        JSONObject value = item.getJSONObject("value");
        if (!value.containsKey("device_key")) {
            log.info("空调数据源不包含键值device_key,丢弃,数据:" + JSON.toJSONString(value));
            return;
        }
        String key = value.getString("device_key");
        if (!this.keys.contains(key)) {
            return;
        }
        AirHandlerCubeItem result = new AirHandlerCubeItem();
        result.Import(value);
        collector.collect(result);
    }
}
