package cn.v5.flume.sink;

import cn.v5.flume.exception.DataOutOfRangeException;
import com.mysql.jdbc.exceptions.MySQLDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by yangwei on 15-4-22.
 */
public class PrepareWrapper {
    private static final Logger logger = LoggerFactory.getLogger(PrepareWrapper.class);

    public static void executeUpdate(TableMeta tm, String[] data) throws SQLException {
        PreparedStatement ps = tm.getPs();
        for (int i = 1; i <= tm.getColumnTypes().size(); i++) {
            TableMeta.ColumnTypeWithIndex typeWithIndex = tm.getColumnTypes().get(i - 1);
            int type = typeWithIndex.getColumnType();
            int index = typeWithIndex.getIndex() - 1;
            switch (type) {
                case Types.VARCHAR:
                case Types.NVARCHAR:
                    if (index >= data.length) {
                        ps.setString(i, "");
                    } else {
                        int size = typeWithIndex.getLen();
                        if (data[index].length() > size) {
                            throw new DataOutOfRangeException("data " + data[index] + " gt max len " + size);
                        }
                        ps.setString(i, data[index]);
                    }
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    if (index >= data.length || data[index].isEmpty()) {
                        ps.setInt(i, 0);
                    } else {
                        int res = Integer.parseInt(data[index]);
                        if (type == Types.TINYINT) {
                            if (res < Byte.MIN_VALUE || res > Byte.MAX_VALUE) {
                                throw new DataOutOfRangeException("data " + data[index] + " out of range tinyint");
                            }
                        } else if (type == Types.SMALLINT) {
                            if (res < Short.MIN_VALUE || res > Short.MAX_VALUE) {
                                throw new DataOutOfRangeException("data " + data[index] + " out of range smallint ");
                            }
                        }
                        ps.setInt(i, res);
                    }
                    break;
                case Types.BIGINT:
                    if (index >= data.length || data[index].isEmpty()) {
                        ps.setLong(i, 0L);
                    } else {
                        ps.setLong(i, Long.parseLong(data[index]));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("type " + type + " not supported");
            }
        }
        ps.addBatch();
    }
}