package com.dsk.storm.state;

import com.dsk.utils.Constants;
import storm.trident.state.StateType;

import java.io.Serializable;

/**
 * User: yanbit
 * Date: 2015/12/16
 * Time: 15:19
 */
public class KuduStateConfig implements Serializable {

    private String tableName;
    private String[] keyColumns;
    private String[] valueColumns;

    private StateType type = StateType.TRANSACTIONAL;

    private String kuduMaster = Constants.KUDU_MASTER;

    // kudu batch
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    private int cacheSize = Constants.DEFAULT_CACHE_SIZE;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(String[] keyColumns) {
        this.keyColumns = keyColumns;
    }

    public String[] getValueColumns() {
        return valueColumns;
    }

    public void setValueColumns(String[] valueColumns) {
        this.valueColumns = valueColumns;
    }

    public StateType getType() {
        return type;
    }

    public void setType(StateType type) {
        this.type = type;
    }

    public String getKuduMaster() {
        return kuduMaster;
    }

    public void setKuduMaster(String kuduMaster) {
        this.kuduMaster = kuduMaster;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
}
