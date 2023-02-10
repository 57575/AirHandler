package AirHandler.utils;

import AirHandler.models.WarningReceiver;
import AirHandler.models.outputs.StrategyAbnormalRecord;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class PostMessage {
    public static void PostWarningMessage(String host, String projectId, String templateId, String receiversJsonStr, StrategyAbnormalRecord item, Logger log) {
        try {
            String route = "/api/messagecenter/projects/{projectId}/messages/templateId/{templateId}";
            String message = "空调设备" + item.SensorKey + "存在温度设置不合理情况;温度设置:" + item.OriginalData.getDouble("temperature_set") + ";模式:" + item.OriginalData.getString("mode");
            HttpResponse<String> response = Unirest.post(host + route)
                    .routeParam("projectId", projectId)
                    .routeParam("templateId", templateId)
                    .header("Content-Type", "application/json")
                    .header("Authorization", GetToken())
                    .body(GetBody(message, item.Time, receiversJsonStr))
                    .asString();
            log.info("发出报警:" + message + ";时间:" + item.Time + ";结果:" + response.getStatus());
        } catch (Exception e) {
            log.error("发出报警失败:" + e.getMessage());
        }
    }

    private static String GetBody(String message, String timeStr, String receiverStr) {
        JSONObject body = new JSONObject();
        JSONObject sender = new JSONObject();
        sender.put("type", "系统");
        sender.put("id", 0);
        sender.put("name", "空调异常检测");
        body.put("sender", sender);

        List<JSONObject> receivers = new ArrayList<>();
        List<WarningReceiver> warningReceiverList = JSONArray.parseArray(receiverStr).toJavaList(WarningReceiver.class);
        for (WarningReceiver p : warningReceiverList
        ) {
            JSONObject receiver = new JSONObject();
            receiver.put("type", p.type);
            receiver.put("id", p.id);
            receiver.put("name", p.name);
            receivers.add(receiver);
        }

        body.put("receivers", receivers);

        body.put("priority", "高");

        JSONObject parameters = new JSONObject();
        parameters.put("name", "空调温度设置异常");
        parameters.put("message", message);
        parameters.put("datetime", timeStr);
        body.put("parameters", parameters);
        return JSON.toJSONString(body);
    }

    private static String GetToken() {
        return "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjEiLCJwaWQiOiIwIiwibmFtZSI6Iui2hee6p-aXoOaVjOeuoeeQhuWRmCIsImNpZCI6IjEiLCJyb290IjoiMSIsIm5iZiI6MTY2ODY3NTI1MiwiZXhwIjoxOTg0MDM1MjUyLCJpYXQiOjE2Njg2NzUyNTIsImlzcyI6IlNDQzQifQ.-4odE4nOqPWEPwZG5QTBz1iOIYqPt4OCTYZySbE0vW4";
    }

}
