package cn.v5.flume.channel;

import cn.v5.flume.utils.MatchPattern;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.AbstractChannelSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by yangwei on 15-4-23.
 */
public class CustomChannelSelector extends AbstractChannelSelector {
    private static final Logger logger = LoggerFactory.getLogger(CustomChannelSelector.class);

    public static final String CONFIG_PREFIX_MAPPING = "mapping.";
    public static final String CONFIG_PREFIX_RULES = "rules.";
    public static final String CONFIG_DEFAULT_CHANNEL = "default";

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory
            .getLogger(CustomChannelSelector.class);

    private static final List<Channel> EMPTY_LIST =
            Collections.emptyList();

    private Map<MatchPattern, List<Channel>> channelMapping;
    private List<Channel> defaultChannels;

    @Override
    public List<Channel> getRequiredChannels(Event event) {
        String[] data;
        try {
            byte[] datas = event.getBody();
            String ds = new String(datas, "UTF-8");

            Iterable<String> components = Splitter.on("|").trimResults().split(ds);
            data = Iterables.toArray(components, String.class);
        } catch (UnsupportedEncodingException e) {
            return defaultChannels;
        }

        List<Channel> channels = Lists.newArrayList();
        for (Map.Entry<MatchPattern, List<Channel>> mapping : channelMapping.entrySet()) {
            if (mapping.getKey().match(data)) {
                channels.addAll(mapping.getValue());
            }
        }

        if (channels.isEmpty()) {
            return defaultChannels;
        }
        return channels;
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        return EMPTY_LIST;
    }

    @Override
    public void configure(Context context) {
        Map<String, String> ruleConfig = context.getSubProperties(CONFIG_PREFIX_RULES);
        Map<String, MatchPattern> rulePattern = Maps.newHashMap();
        for (Map.Entry<String, String> rule : ruleConfig.entrySet()) {
            MatchPattern matchPattern = new MatchPattern();
            matchPattern.parsePattern(rule.getValue());
            rulePattern.put(rule.getKey(), matchPattern);
        }
        logger.debug("rule pattern is {}", rulePattern);

        Map<String, Channel> channelNameMap = getChannelNameMap();

        defaultChannels = getChannelListFromNames(context.getString(CONFIG_DEFAULT_CHANNEL), channelNameMap);
        logger.debug("default channels are {}", defaultChannels);

        Map<String, String> mapConfig = context.getSubProperties(CONFIG_PREFIX_MAPPING);

        channelMapping = Maps.newHashMap();

        for (String ruleValue : mapConfig.keySet()) {
            List<Channel> configuredChannels = getChannelListFromNames(
                    mapConfig.get(ruleValue),
                    channelNameMap);

            //This should not go to default channel(s)
            //because this seems to be a bad way to configure.
            if (configuredChannels.size() == 0) {
                throw new FlumeException("No channel configured for when "
                        + "rule value is: " + ruleValue);
            }

            MatchPattern rp = rulePattern.get(ruleValue);
            Preconditions.checkNotNull(rp, "rule " + ruleValue + " not exists");
            if (channelMapping.put(rp, configuredChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
            logger.debug("channels for {} are {}", ruleValue, configuredChannels);
        }
    }
}
