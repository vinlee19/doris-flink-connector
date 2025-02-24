package org.apache.doris.flink.tools.cdc;

import java.util.ArrayList;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class PgTest {
    public static void main(String[] args) {
        Column column1 = new Column(1, "fid");
        Column column2 = new Column(1, "fid");
        Column column3 = new Column(2, "fid");

        ArrayList<Column> columnArrayList = new ArrayList<>();
        columnArrayList.add(column1);
        columnArrayList.add(column2);
        columnArrayList.add(column3);

        Map<Integer, String> stringMap =
                columnArrayList.stream()
                        .filter(column -> column.getId() == 1)
                        .collect(toMap(Column::getId, Column::getName, (r1, r2) -> r1));
    }
}
