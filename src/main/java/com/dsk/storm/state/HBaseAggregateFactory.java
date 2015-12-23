package com.dsk.storm.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.dsk.utils.TridentConfig;
import org.apache.log4j.Logger;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.*;

import java.util.Map;

/**
 * Factory for creating {@link HBaseAggregateState} objects for Trident
 */
@SuppressWarnings({"serial", "rawtypes", "unchecked"})
public class HBaseAggregateFactory implements StateFactory {
    private static final Logger LOG = Logger.getLogger(HBaseAggregateFactory.class);
    private StateType type;
    private TridentConfig config;

    /**
     * @param config The {@link TridentConfig}
     * @param type   The {@link StateType}
     */
    public HBaseAggregateFactory(final TridentConfig config, final StateType type) {
        this.config = config;
        this.type = type;

        if (config.getStateSerializer() == null) {
            config.setStateSerializer(TridentConfig.DEFAULT_SERIALZERS.get(type));
            if (config.getStateSerializer() == null) {
                throw new RuntimeException("Unable to find serializer for state type: " + type);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting default serializer: " + config.getStateSerializer().getClass().getName());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        HBaseAggregateState state = new HBaseAggregateState(config);
        CachedMap c = new CachedMap(state, config.getStateCacheSize());

        MapState ms;
        if (type == StateType.NON_TRANSACTIONAL) {
            ms = NonTransactionalMap.build(c);
        } else if (type == StateType.OPAQUE) {
            ms = OpaqueMap.build(c);
        } else if (type == StateType.TRANSACTIONAL) {
            ms = TransactionalMap.build(c);
        } else {
            throw new RuntimeException("Unknown state type: " + type);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating new HBaseState: " + type);
        }

        return new SnapshottableMap(ms, new Values("$GLOBAL$"));
    }
}
