package cn.v5.flume.sink;

import cn.v5.flume.utils.MatchPattern;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;

/**
 * Created by yangwei on 15-4-20.
 */
public class MysqlSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);
    private static final Logger backLogger = LoggerFactory.getLogger("MYSQL_SINK_BACK");

    private String dbUrl;
    private String dbName;
    private String dbPass;

    private int batchSize;

    private Connection conn;

    private Multimap<MatchPattern, TableMeta> sqlMapper;

    private CounterGroup counterGroup;

    private long startTime;

    private long insertCount = 0;

    public MysqlSink() {
        this.counterGroup = new CounterGroup();
        this.sqlMapper = ArrayListMultimap.create();
        startTime = System.currentTimeMillis();
    }

    private void createConnectionAndPrepare() throws SQLException {
        if (conn == null) {
            logger.info("Creating conneciton to: {}", dbUrl);
            conn = DriverManager.getConnection(dbUrl, dbName, dbPass);
            //conn.setAutoCommit(false);
            conn.setAutoCommit(true);

            reloadTables(true);
        }
    }

    private void reloadTables(boolean force) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        Multimap<MatchPattern, TableMeta> cloneSqlMapper = ArrayListMultimap.create(sqlMapper);
        for (Map.Entry<MatchPattern, Collection<TableMeta>> dbMapper : cloneSqlMapper.asMap().entrySet()) {
            MatchPattern pattern = dbMapper.getKey();
            Collection<TableMeta> tms = dbMapper.getValue();
            if (!pattern.hasPartition()) {
                if (force) {
                    TableMeta tm = tms.iterator().next();
                    initTableMeta(tm, metaData);
                }
            } else {
                TableMeta firstTable = tms.iterator().next();
                String baseTableName = firstTable.getBaseTableName();
                logger.info("{}: Get partition tables {}", getName(), baseTableName);
                ResultSet rs = metaData.getTables(null, null, "%" + baseTableName + "%", null);
                while (rs.next()) {
                    logger.info("{}: Get partition table {}", getName(), rs.getString(3));
                    TableMeta curTableMeta = null;
                    for (TableMeta tm : tms) {
                        if (tm.getTableName().compareTo(rs.getString(3)) == 0) {
                            curTableMeta = tm;
                            break;
                        }
                    }
                    if (curTableMeta != null && curTableMeta.getPs() != null && !curTableMeta.getPs().isClosed()) {
                        continue;
                    }
                    String suffix = rs.getString(3).substring(baseTableName.length());
                    if (StringUtils.isNotEmpty(suffix)) {
                        String partition = suffix.substring(1);
                        if (!pattern.isValidPartition(partition)) {
                            logger.info("{}: Table {} is not a partition table, ignore it", getName(), rs.getString(3));
                            continue;
                        }
                    } else {
                        logger.info("{}: Table {} is not a partition table, ignore it", getName(), rs.getString(3));
                        continue;
                    }
                    // Just re init old table meta
                    if (curTableMeta != null) {
                        initTableMeta(curTableMeta, metaData);
                    } else {
                        TableMeta newTable = new TableMeta(baseTableName, suffix, firstTable.getBaseInsertString(), firstTable.getIndexes());
                        initTableMeta(newTable, metaData);
                        sqlMapper.put(pattern, newTable);
                    }
                }
            }
        }
    }

    private void initTableMeta(TableMeta tm, DatabaseMetaData metaData) throws SQLException {
        logger.info("{}: Prepare statements of {}", getName(), tm.getTableName());

        ResultSet columnResults = metaData.getColumns(null, "%", tm.getTableName(), "%");
        PreparedStatement ps = conn.prepareStatement(tm.getInsertString());
        tm.init(ps, columnResults);
        logger.info(getName() + ": init table {} column metas {}", tm.getTableName(), tm.getColumnTypes());
    }

    private void destroyConnection() {
        if (conn != null) {
            logger.info(getName() + ": Closing prepare statement of all tables");
            for (TableMeta tm : sqlMapper.values()) {
                PreparedStatement ps = tm.getPs();
                if (ps != null) {
                    try {
                        tm.setPs(null);
                        ps.close();
                    } catch (SQLException e) {
                        logger.warn(getName() + ": Close prepare statement of " + tm.getTableName() + " exception:", e);
                    }
                }
            }

            try {
                logger.info("Destroying connection to: {}", dbUrl);
                conn.close();
            } catch (SQLException e) {
                logger.warn("Close connection exception:", e);
            }
        }

        conn = null;
    }

    @Override
    public synchronized void start() {
        logger.info("Mysql sink {} starting", getName());

        try {
            createConnectionAndPrepare();
        } catch (Exception e) {
            logger.error(getName() + " Unable to create mysql client using " + dbUrl + ". Exception:", e);
            destroyConnection();
            return;
        }

        super.start();

        logger.info("Mysql sink {} started", this.getName());
    }

    @Override
    public synchronized void stop() {
        logger.info("Mysql sink {} stopping", getName());

        destroyConnection();

        super.stop();

        logger.info("Mysql sink {} stopped. Metrics: {}", this.getName(), counterGroup);
    }

    private TableMeta insertEvent(String event) throws SQLException {
        Iterable<String> components = Splitter.on("|").trimResults().split(event);
        String[] data = Iterables.toArray(components, String.class);

        MatchPattern curPattern = null;
        for (Map.Entry<MatchPattern, Collection<TableMeta>> dbMapper : sqlMapper.asMap().entrySet()) {
            MatchPattern m = dbMapper.getKey();
            if (m.match(data)) {
                if (!m.hasPartition()) {
                    // TODO Is is possible to match a few tables in the case of nopartition?
                    PrepareWrapper.executeUpdate(dbMapper.getValue().iterator().next(), data);
                    return dbMapper.getValue().iterator().next();
                } else {
                    for (TableMeta tm : dbMapper.getValue()) {
                        if (StringUtils.isEmpty(tm.getSuffix())) {
                            // skip the dummy table
                            continue;
                        }
                        // trim "_" to get date partition
                        String partition = tm.getSuffix().substring(1);
                        if (m.getPartition(data).compareTo(partition) == 0) {
                            PrepareWrapper.executeUpdate(tm, data);
                            return tm;
                        }
                    }
                    curPattern = m;
                    break;
                }
            }
        }

        // No partition table, we need to reload metadata
        if (curPattern != null) {
            logger.info(String.format("%s: Insert event %s not found partition %s of the table %s\n", getName(), event,
                    curPattern.getPartition(data),
                    sqlMapper.get(curPattern).iterator().next().getBaseTableName()));
            reloadTables(false);
            for (TableMeta tm : sqlMapper.get(curPattern)) {
                if (StringUtils.isEmpty(tm.getSuffix())) {
                    // skip the dummy table
                    continue;
                }
                String partition = tm.getSuffix().substring(1);
                if (curPattern.getPartition(data).compareTo(partition) == 0) {
                    PrepareWrapper.executeUpdate(tm, data);
                    return tm;
                }
            }
            // Not found partition table, record the log
            backLogger.error(event);
        }
        return null;
    }

    @Override
    public void configure(Context context) {
        dbUrl = context.getString("dburl");
        dbName = context.getString("dbuser");
        dbPass = context.getString("dbpass");

        this.batchSize = Integer.parseInt(context.getString("batch_size", "100"));

        Preconditions.checkNotNull(dbUrl, "db url cannot be empty");

        String tables = context.getString("dbmapper");
        if (tables != null && !tables.isEmpty()) {
            String[] tableToken = tables.split(" ");
            for (int i = 0; i < tableToken.length; i++) {
                logger.info("loading table mapper " + tableToken[i]);
                String key = context.getString("dbmapper." + tableToken[i] + ".pattern");
                String fields = context.getString("dbmapper." + tableToken[i] + ".fields");
                String value = context.getString("dbmapper." + tableToken[i] + ".prepare");
                String partition = context.getString("dbmapper." + tableToken[i] + ".partition", "");
                Preconditions.checkNotNull(key, "db mapper pattern cannot be empty");
                Preconditions.checkNotNull(fields, "db mapper fields cannot be empty");
                Preconditions.checkNotNull(value, "db mapper prepare cannot be empty");
                String table = tableToken[i];
                if (value.startsWith("insert into ")) {
                    Iterable<String> prepareFields = Splitter.on(" ").omitEmptyStrings().split(value);
                    int k = 0;
                    for (String prepareField : prepareFields) {
                        if (k == 2) {
                            table = prepareField;
                            if (table.endsWith("(")) {
                                table = table.substring(0, table.length() - 1);
                            }
                            break;
                        }
                        k++;
                    }
                }

                MatchPattern matchPattern;
                Iterable<String> partitionComponents = Splitter.on(":").omitEmptyStrings().split(partition);
                String[] data = Iterables.toArray(partitionComponents, String.class);
                if (data.length != 2) {
                    matchPattern = new MatchPattern();
                } else {
                    matchPattern = new MatchPattern(Integer.parseInt(data[0]), data[1]);
                }
                //logger.debug(String.format("table %s, pattern %s, fields %s, prepare %s, partition %s\n", table, key, fields, value, partition));
                matchPattern.parsePattern(key);
                // If table has partition, then we put a dummy table meta to init partition tables
                sqlMapper.put(matchPattern, new TableMeta(table, value, Splitter.on(",").trimResults().split(fields)));
            }
        }
    }

    private TableMeta sendLine(Event event) throws SQLException {
        byte[] datas = event.getBody();
        String ds = null;
        try {
            ds = new String(datas, "UTF-8");
            if (logger.isTraceEnabled()) {
                logger.trace(getName() + ": " + ds);
            }
            TableMeta deal = insertEvent(ds);
            if (deal == null) {
                logger.warn("{}: event body {} not match any sql mapper", getName(), ds);
            }
            return deal;
        } catch (UnsupportedEncodingException uee) {
            logger.error("{}: converting string failed while handling data {}", getName(), ds);
            backLogger.error(ds);
        } catch (SQLException e) {
            if (e.getCause() instanceof SQLNonTransientException) {
                logger.error(getName() + ": insert event " + ds + " failed: ", e);
                backLogger.error(ds);
            } else {
                throw e;
            }
        } catch (Exception e) {
            logger.error(getName() + ": insert event " + ds + " failed: ", e);
            backLogger.error(ds);
        } finally {
        }
        return null;
    }

    private void appendBatch(List<Event> events) throws SQLException {
        Set<TableMeta> batchTables = Sets.newHashSet();
        for (Event event : events) {
            TableMeta table = sendLine(event);
            if (table != null) {
                insertCount++;
                batchTables.add(table);
            }
        }

        for (TableMeta table : batchTables) {
            try {
                table.getPs().executeBatch();
            } catch (SQLException e) {
                logger.error(getName() + ": Execute batch insertion of " + table.getTableName() + " failed");

                /**
                 * The subclass of {@link SQLException} thrown when an instance where a retry
                 * of the same operation would fail unless the cause of the <code>SQLException</code>
                 * is corrected.
                 */
                if (e.getCause() instanceof SQLNonTransientException) {
                    logger.error(getName() + ": Execute batch insertion of " + table.getTableName() + " failed: ", e);
                } else {
                    throw e;
                }
            }
        }

        //conn.commit();
        long duration = System.currentTimeMillis() - startTime;
        logger.debug(getName() + ": Commits {} events in {} ms", insertCount, duration);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();
            createConnectionAndPrepare();

            List<Event> batch = Lists.newLinkedList();

            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();

                if (event == null) {
                    break;
                }

                batch.add(event);
            }

            int size = batch.size();

            if (size == 0) {
                status = Status.BACKOFF;
            } else {
                appendBatch(batch);
            }

            transaction.commit();

        } catch (ChannelException e) {
            transaction.rollback();
            logger.error(getName() +
                    ": Unable to get event from channel. Exception follows.", e);
            status = Status.BACKOFF;
        } catch (Exception e) {
            transaction.rollback();
            logger.error(getName() +
                            ": Unable to communicate with mysql server. Exception follows.",
                    e);
            status = Status.BACKOFF;
            /*try {
                conn.rollback();
            } catch (SQLException se) {
                logger.error("rollback batch events failed {}", se);
            }*/
            destroyConnection();
        } finally {
            transaction.close();
        }

        return status;
    }
}
