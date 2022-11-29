package AirHandler.Functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;

public class CubeItemFilterInvalidFunction implements FilterFunction<JSONObject> {
    private final Logger LOG;

    public CubeItemFilterInvalidFunction(Logger log) {
        LOG = log;
    }

    @Override
    public boolean filter(JSONObject jsonObject) throws Exception {
        if (!jsonObject.containsKey("value")) {
            LOG.info("空调数据源不包含value,丢弃,数据:" + JSON.toJSONString(jsonObject));
            return false;
        } else {
            if (!jsonObject.getJSONObject("value").containsKey("device_key")) {
                LOG.info("空调数据源不包含键值device_key,丢弃,数据:" + JSON.toJSONString(jsonObject));
                return false;
            }
            if (!jsonObject.getJSONObject("value").containsKey("time")) {
                LOG.info("空调数据源不包含时间time,丢弃,数据:" + JSON.toJSONString(jsonObject));
                return false;
            }
            return true;
        }
    }
}
