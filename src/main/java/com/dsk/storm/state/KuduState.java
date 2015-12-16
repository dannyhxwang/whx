package com.dsk.storm.state;

import backtype.storm.task.IMetricsContext;
import org.kududb.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.*;

import java.util.List;
import java.util.Map;

/**
 * User: yanbit
 * Date: 2015/12/15
 * Time: 15:12
 */


public class KuduState<T> implements IBackingMap<T> {
    private static final Logger logger = LoggerFactory.getLogger(KuduState.class);

    private KuduClient kuduClient;
    private KuduStateConfig config;

    public KuduState(final KuduStateConfig config) {
        this.config = config;
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(config.getKuduMaster()).build();
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        System.out.println("gggggggggggggggggggggkeys"+keys);
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        System.out.println("ppppppppppppppppppppkeys"+keys);
        System.out.println("ppppppppppppppppppppvalues"+vals);
    }

    public static Factory newFactory(final KuduStateConfig config) {
        return new Factory(config);
    }

    static class Factory implements StateFactory {
        private KuduStateConfig config;

        Factory(final KuduStateConfig config) {
            this.config = config;
        }

        @Override
        public State makeState(final Map conf, final IMetricsContext context, final int partitionIndex, final int numPartitions) {
            final CachedMap map = new CachedMap(new KuduState(config), config.getCacheSize());
            switch (config.getType()) {
                case OPAQUE:
                    return OpaqueMap.build(map);
                case TRANSACTIONAL:
                    return TransactionalMap.build(map);
                default:
                    return NonTransactionalMap.build(map);
            }
        }
    }
}
