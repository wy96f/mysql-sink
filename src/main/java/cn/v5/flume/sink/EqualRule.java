package cn.v5.flume.sink;

import com.google.common.base.Objects;

/**
 * Created by yangwei on 4/8/15.
 */
public class EqualRule implements IRule {
    String expect;

    public EqualRule(String expect) {
        this.expect = expect;
    }

    @Override
    public boolean match(String field) {
        return field.compareTo(expect) == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EqualRule equalRule = (EqualRule) o;
        return Objects.equal(expect, equalRule.expect);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expect);
    }
}
