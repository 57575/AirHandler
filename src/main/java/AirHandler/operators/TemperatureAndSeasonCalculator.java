package AirHandler.operators;

import AirHandler.enums.AirHandlerMode;
import AirHandler.models.CubeItem;
import AirHandler.models.ResultItem;
import scala.Int;

import java.util.*;

public class TemperatureAndSeasonCalculator {
    private static List<Integer> Summer = new ArrayList<>(Arrays.asList(5, 6, 7, 8, 9, 10));
    private static List<Integer> Winter = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 11, 12));
    private static double SummerTem = 26;
    private static double WinterTem = 20;

    public static ResultItem Calculate(CubeItem item) {
        ResultItem result = new ResultItem();
        result.SetName(item.DeviceKey);
        result.SetMode(item.Mode);
        result.SetTemperature(item.Temperature);
        result.SetTemperatureSet(item.TemperatureSet);
        result.SetStatus(item.Status);
        result.SetTimeStamp(item.TimeStamp);
        result.SetIsWarning(CalculateWarning(item));

        return result;
    }

    private static boolean CalculateWarning(CubeItem item) {
        if (item.Status) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
            calendar.setTimeInMillis(item.TimeStamp);
            int month = calendar.get(Calendar.MONTH) + 1;

            //夏天
            if (Summer.stream().anyMatch(x -> x == month)) {
                if (item.TemperatureSet < SummerTem && item.Mode.equals(AirHandlerMode.制冷.toString())) {
                    return true;
                } else {
                    return false;
                }
            }
            //冬天
            else {
                if (item.TemperatureSet > WinterTem && item.Mode.equals(AirHandlerMode.制热.toString())) {
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    public static void SetSeason(List<Integer> summer, List<Integer> winter) {
        Summer = summer;
        Winter = winter;
    }

    public static void SetTemperature(double summerTem, double winterTem) {
        SummerTem = summerTem;
        WinterTem = winterTem;
    }

}
