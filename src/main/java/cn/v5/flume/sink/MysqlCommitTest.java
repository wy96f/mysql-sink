package cn.v5.flume.sink;

import cn.v5.flume.utils.Pair;
import com.google.common.base.Splitter;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yangwei on 31/7/15.
 */
public class MysqlCommitTest {
    private static final Logger logger = LoggerFactory.getLogger(MysqlCommitTest.class);
    private static final Logger backLogger = LoggerFactory.getLogger("back");

    private static Connection conn;

    private static PreparedStatement ps;

    private static String dbUrl = "jdbc:mysql://localhost/chatgame_log?useUnicode=true&autoReconnect=true&rewriteBatchedStatements=true";

    private static void createConnectionAndPrepare() throws SQLException {
        String dbName = "root";
        String dbPass = "";
        if (conn == null) {
            logger.info("Creating conneciton to: {}", dbUrl);
            conn = DriverManager.getConnection(dbUrl, dbName, dbPass);
            conn.setAutoCommit(false);
        }

        DatabaseMetaData metaData = conn.getMetaData();

        ResultSet rs = metaData.getTables(null, null, "%transaction_test", null);
        while (rs.next()) {
            System.out.println(rs.getString(3));
        }

        ResultSet resultSet = metaData.getColumns(null, "%", "transaction_test_myisam", "%");
        while (resultSet.next()) {
            String name = resultSet.getString("COLUMN_NAME");
            String type = resultSet.getString("TYPE_NAME");
            int dataType = resultSet.getInt("DATA_TYPE");
            int size = resultSet.getInt("COLUMN_SIZE");
            String dvalue=resultSet.getString("COLUMN_DEF");

            System.out.println("Column name: [" + name + "]; type: [" + type
                    + "]; dataType: [" + dataType + "]; size: [" + size + "]"+"; defaultvalue:["+dvalue+"];");
        }

        ps = conn.prepareStatement("INSERT INTO transaction_test_myisam(val, p2p_result) VALUES (?, ?)");
    }

    private static void destroyConnection() {
        if (conn != null) {
            try {
                logger.info("Destroying connection to: {}", dbUrl);
                conn.close();
            } catch (SQLException e) {
                logger.warn("close connection exception:", e);
            }
        }

        conn = null;
    }

    public static void main(String[] args) throws SQLException, ParseException {
        logger.debug("main");
        backLogger.debug("back");
        java.sql.Timestamp timestamp = new java.sql.Timestamp(Long.parseLong("1441282639874"));
        System.out.println(new SimpleDateFormat("yyyy_MM_dd").format(timestamp));
        String[] pattern = {"yyyy_MM_dd"};
        //DateUtils.parseDateStrictly("2016_19_12", pattern);

                 Matcher m = Pattern.compile("^(?!/api.*register).*$").matcher("");
        m.reset("/api/user/message/unread");
        m.reset("/api/user/register");
        m.reset("/api/user//register");
        System.out.println(m.matches());

        createConnectionAndPrepare();

        ps.setString(1, "x");
        ps.setInt(2, -128);
        ps.addBatch();

        ps.setString(1, "y");
        ps.setInt(2, -127);
        ps.addBatch();

        ps.setString(1, "z");
        ps.setInt(2, 256);
        ps.addBatch();

        ps.executeBatch();

        conn.commit();

        destroyConnection();
    }
}
