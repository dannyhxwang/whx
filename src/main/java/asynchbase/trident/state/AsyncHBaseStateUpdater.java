/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package asynchbase.trident.state;

import asynchbase.trident.mapper.IAsyncHBaseTridentFieldMapper;
import asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Update an AsyncHBaseState<br/>
 * Use this function with Stream.partitionAggregate(State,Aggregator,StateUpdater,...)
 */
public class AsyncHBaseStateUpdater extends BaseStateUpdater<AsyncHBaseState> {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseStateUpdater.class);

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        for (IAsyncHBaseTridentFieldMapper fieldMapper : mapper.getFieldMappers()) {
            fieldMapper.prepare(conf);
        }
    }

    private final IAsyncHBaseTridentMapper mapper;
    private boolean throttle = false;
    private boolean async = true;

    public AsyncHBaseStateUpdater(IAsyncHBaseTridentMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param async Whether or not to wait for
     * @return this so you can do method chaining
     */
    public AsyncHBaseStateUpdater setAsync(boolean async) {
        this.async = async;
        return this;
    }

    /**
     * @param state     AsyncHBaseState to update.
     * @param tuples    Storm Tuples to process.
     * @param collector Unused as PutRequest returns void.
     */
    @Override
    public void updateState(final AsyncHBaseState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {

        log.info("OpenTsdbStateUpdater : " + "Saving " + tuples.size() + " tuples to OpenTSDB");
        long start_time = System.currentTimeMillis();

        Callback<Object, Exception> errback = new Callback<Object, Exception>() {
            @Override
            public Object call(Exception ex) throws Exception {
                log.warn("AsyncHBase failure ", ex);
                if (ex instanceof PleaseThrottleException) {
                    throttle = true;
                    return ex;
                }
                synchronized (collector) {
                    collector.reportError(ex);
                }
                return ex;
            }
        };

        List<Deferred<ArrayList<Object>>> results = new ArrayList<Deferred<ArrayList<Object>>>();
        for (final TridentTuple tuple : tuples) {
            results.add(state.updateState(mapper, tuples).addErrback(errback));
        }

        if (throttle) {
            log.warn("Throttling...");
            long throttle_time = System.nanoTime();
            try {
                Deferred.group(results).join();
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.toString());
                collector.reportError(ex);
            } finally {
                throttle_time = System.nanoTime() - throttle_time;
                if (throttle_time < 1000000000L) {
                    log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        log.error("AsyncHBase exception : " + ex.toString());
                    }
                }
                log.info("Done throttling...");
                this.throttle = false;
            }
        } else if (!async) {
            try {
                Deferred.group(results).join();
            } catch (InterruptedException ex) {
                log.warn("OpenTSDB results join exception ", ex);
            } catch (Exception ex) {
                log.warn("OpenTSDB exception ", ex);
            }
        }

        long elapsed = System.currentTimeMillis() - start_time;
        log.info("OpenTsdbStateUpdater : " + tuples.size() + " tuples saved to OpenTSDB in " + elapsed + "ms");
    }
}
