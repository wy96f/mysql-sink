package cn.v5.flume.sink;

/**
 * Created by yangwei on 4/8/15.
 */
public interface IRule {
    boolean match(String field);
}