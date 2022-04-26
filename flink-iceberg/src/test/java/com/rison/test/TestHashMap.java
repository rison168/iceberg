package com.rison.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.table.data.TimestampData;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @PACKAGE_NAME: com.rison.iceberg.flink.cdc.mysql.api
 * @NAME: TestHashMap
 * @USER: Rison
 * @DATE: 2022/4/24 9:23
 * @PROJECT_NAME: iceberg
 **/
public class TestHashMap {
    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        map.put("lixiao", "lixiao");
        String lixiao = map.get("LIXIAO");
        System.out.println(lixiao);
        Map result = new CaseInsensitiveMap();
        result.put("lixiao", "lixiao-caseInsesitve");
        System.out.println(result.get("lixiAo"));
        System.out.println(result.get("lixiAo1"));
    }
    @Test
    public void test01(){
        String str = "decimal(10, 2)";
        String str2 = "decimal";
        System.out.println(str.substring(0, str.indexOf("(")));
//        System.out.println(str2.substring(0, str2.indexOf("(")));
    }

    @Test
    public void test02() throws ParseException {
        String date1 = "2021-01-02";
        String date2 = "2021-01-02 12:12:12";
        System.out.println(getTimeStampData(date1));
        System.out.println(getTimeStampData(date2));
    }
    public static TimestampData getTimeStampData(String timeStr) throws ParseException {
        String dateStr = timeStr.replace("T", " ");
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        long timestamp = format.parse(dateStr).getTime() + 28800000;
        return TimestampData.fromEpochMillis(timestamp);
    }

    @Test
    public void test03 (){
        String data = "{\"table\":\"ABC.OMS_SSDL\",\"op_type\":\"I\",\"op_ts\":\"2021-12-02 02:11:43.000000\",\"current_ts\":\"2021-12-08T14:16:50.786019\",\"pos\":\"00000000000083172328\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":\"c9ccd8801d369673d66fbc6cc1ee1bef\",\"REQ_DATE\":\"2021-12-01 00:00:00.000000000\",\"PLAN_ELECTRIC237\":\"0\",\"REAL_ELECTRIC237\":\"116\",\"PLAN_ELECTRIC723\":\"0\",\"REAL_ELECTRIC723\":\"255\",\"NAME\":\"海南受广东\",\"ALLDAY_PLAN_ELECTRIC\":\"0\",\"ALLDAY_REAL_ELECTRIC\":\"371\",\"REAL_ZDDL\":\"131\",\"PLAN_ZDDL\":\"0\",\"CREATE_TIME\":\"2021-12-02 07:20:10.000000000\"}}\n";
        JSONObject jsonObject = JSON.parseObject(data);
        JSONObject after = jsonObject.getJSONObject("after");
        System.out.println(after);
        System.out.println(after.get("id"));
        System.out.println(after.get("ID"));
        System.out.println(after.containsKey("id"));
        System.out.println(after.containsKey("ID"));

        String id = "str";
        System.out.println(id.toUpperCase());
        System.out.println(id.toLowerCase());
    }
}
