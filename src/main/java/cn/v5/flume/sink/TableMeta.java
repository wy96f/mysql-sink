package cn.v5.flume.sink;

import cn.v5.flume.utils.Pair;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by yangwei on 15-4-22.
 */
public class TableMeta {
    private final String tableName;
    private final String insertString;
    private final String suffix;
    private final Iterable<String> indexes;

    private PreparedStatement ps;

    private List<String> columnNames;
    private List<ColumnTypeWithIndex> columnTypes;

    public TableMeta(String tableName, String insertString, Iterable<String> indexes) {
        this(tableName, "", insertString, indexes);
    }

    public TableMeta(String tableName, String suffix, String insertString, Iterable<String> indexes) {
        this.tableName = tableName;
        this.suffix = suffix;
        this.insertString = insertString;
        this.indexes = indexes;
        parseInsertStringAndFields(indexes);
    }

    public void init(PreparedStatement ps, ResultSet columnResults) throws SQLException {
        this.ps = ps;
        Map<String, Pair<Integer, Integer>> nameToTypes = Maps.newHashMap();
        while (columnResults.next()) {
            String columnName = columnResults.getString("COLUMN_NAME");
            int dataType = columnResults.getInt("DATA_TYPE");
            int size = columnResults.getInt("COLUMN_SIZE");
            nameToTypes.put(columnName, Pair.create(dataType, size));
        }
        for (int i = 0; i < columnNames.size(); i++) {
            String column = columnNames.get(i);
            Pair<Integer, Integer> attr = nameToTypes.get(column);
            Preconditions.checkNotNull(attr, "not found column attr for " + column + " in table " + getTableName());
            columnTypes.get(i).setColumnType(attr.left);
            columnTypes.get(i).setLen(attr.right);
        }
    }

    public PreparedStatement getPs() {
        return ps;
    }

    public void setPs(PreparedStatement ps) {
        this.ps = ps;
    }

    public List<ColumnTypeWithIndex> getColumnTypes() {
        return columnTypes;
    }

    public Iterable<String> getIndexes() {
        return indexes;
    }

    public String getTableName() {
        return tableName + suffix;
    }

    public String getBaseTableName() {
        return tableName;
    }

    public String getInsertString() {
        return insertString.replaceFirst(tableName, tableName + suffix);
    }

    public String getBaseInsertString() {
        return insertString;
    }

    public String getSuffix() {
        return suffix;
    }

    private void parseInsertStringAndFields(Iterable<String> indexes) {
        int start = insertString.indexOf("(");
        int end = insertString.indexOf(")");
        Iterable<String> cns = Splitter.on(",").trimResults().split(insertString.substring(start + 1, end));
        columnNames = Lists.newArrayList();
        columnTypes = Lists.newArrayList();

        Iterator<String> indexIter = indexes.iterator();
        for (String columnName : cns) {
            if (!indexIter.hasNext()) {
                throw new IllegalArgumentException("has no filed index for col " + columnName);
            }
            columnNames.add(columnName);
            columnTypes.add(new ColumnTypeWithIndex(Integer.parseInt(indexIter.next())));
        }
    }

    protected static class ColumnTypeWithIndex {
        private int index;
        private int columnType;
        private int len;

        private ColumnTypeWithIndex(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public int getColumnType() {
            return columnType;
        }

        public void setColumnType(int columnType) {
            this.columnType = columnType;
        }

        public int getLen() {
            return len;
        }

        public void setLen(int len) {
            this.len = len;
        }

        @Override
        public String toString() {
            return "ColumnTypeWithIndex{" +
                    "index=" + index +
                    ", columnType=" + columnType +
                    '}';
        }
    }
}
