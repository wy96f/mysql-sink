package cn.v5.flume.utils;

import cn.v5.flume.sink.EqualRule;
import cn.v5.flume.sink.IRule;
import cn.v5.flume.sink.RegexRule;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by yangwei on 4/8/15.
 */
public class MatchPattern {
    private static final Logger logger = LoggerFactory.getLogger(MatchPattern.class);

    private static final String EQUAL = "==";
    private static final String REGEX = "=~";

    private static final String FIELD_SEP = "&";

    private int partitionIndex = -1;
    private String format = "";

    public MatchPattern() {
    }

    public MatchPattern(int partitionIndex, String format) {
        this.partitionIndex = partitionIndex;
        this.format = format;
    }

    private final List<Pair<Integer, IRule>> rules = Lists.newArrayList();

    public void parsePattern(String pattern) {
        Iterable<String> fieldPatterns = Splitter.on(FIELD_SEP).omitEmptyStrings().trimResults().split(pattern);

        fieldPatterns.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                if (s.contains(EQUAL)) {
                    String[] fieldPattern = s.split(EQUAL);
                    Preconditions.checkArgument(fieldPattern.length == 2);
                    rules.add(Pair.create(Integer.parseInt(fieldPattern[0]), new EqualRule(fieldPattern[1])));
                    logger.info("add EqualRule: index is {}, expect is {}", fieldPattern[0], fieldPattern[1]);
                } else if (s.contains(REGEX)) {
                    String[] fieldPattern = s.split(REGEX);
                    Preconditions.checkArgument(fieldPattern.length == 2);
                    rules.add(Pair.create(Integer.parseInt(fieldPattern[0]), new RegexRule(fieldPattern[1])));
                    logger.info("add RegexRule: index is {}, expect is {}", fieldPattern[0], fieldPattern[1]);
                } else {
                    throw new IllegalArgumentException("parse pattern " + s + " failed");
                }
            }
        });
    }

    public boolean hasPartition() {
        return partitionIndex != -1;
    }

    public String getPartition(String[] fields) {
        String time = fields[partitionIndex - 1];
        java.sql.Timestamp timestamp = new java.sql.Timestamp(Long.parseLong(time));
        return FastDateFormat.getInstance(format).format(timestamp);
    }

    public boolean isValidPartition(String partition) {
        String[] pattern = {format};
        try {
            DateUtils.parseDateStrictly(partition, pattern);
        } catch (ParseException e) {
            return false;
        }

        return true;
    }

    String getPartition(String time) {
        java.sql.Timestamp timestamp = new java.sql.Timestamp(Long.parseLong(time));
        return FastDateFormat.getInstance(format).format(timestamp);
    }

    public boolean match(String[] fields) {
        for (Pair<Integer, IRule> rule : rules) {
            if (rule.left <= fields.length && rule.right.match(fields[rule.left - 1])) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchPattern that = (MatchPattern) o;
        return Objects.equal(partitionIndex, that.partitionIndex) &&
                Objects.equal(format, that.format) &&
                Objects.equal(rules, that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitionIndex, format, rules);
    }
}
