package org.apache.doris.flink.tools.cdc;

public class ConfigParamsTest {
    public static void main(String[] args) {
        String param = "port=1521";
        String[] split = param.split("=", 2);
        System.out.println(split.length);
        System.out.println(split[0]);
        System.out.println(split[1]);
    }
}
