package cn.v5.flume.sink;

import com.google.common.base.Objects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yangwei on 4/8/15.
 */
public class RegexRule implements IRule {
    Matcher matcher;
    private final String pattern;

    public RegexRule(String pattern) {
        this.pattern = pattern;
        this.matcher = Pattern.compile(pattern).matcher("");
    }

    @Override
    public boolean match(String field) {
        matcher.reset(field);
        return matcher.find();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegexRule regexRule = (RegexRule) o;
        return Objects.equal(pattern, regexRule.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pattern);
    }
}
