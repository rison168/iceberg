package com.rison.test;

import org.apache.commons.collections.map.CaseInsensitiveMap;

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
}
