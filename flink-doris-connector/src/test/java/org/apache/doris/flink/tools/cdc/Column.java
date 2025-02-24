package org.apache.doris.flink.tools.cdc;

import java.util.Objects;

public class Column {
    private int id;
    private String name;

    public Column(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column = (Column) o;
        return id == column.id && Objects.equals(name, column.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "Column{" + "id=" + id + ", name='" + name + '\'' + '}';
    }
}
