package it.unitn.ds1.project.model;

import java.util.concurrent.atomic.AtomicBoolean;

public class DataItem {
    private Integer value;
    private Integer version;
    private final AtomicBoolean locked = new AtomicBoolean(false);

    public DataItem(Integer value, Integer version) {
        this.value = value;
        this.version = version;
    }

    public DataItem(DataItem other) {
        this.version = other.version + 1;
        this.value = other.value;
    }

    public Integer getValue() {
        return this.value;
    }

    public Integer getVersion() {
        return this.version;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public boolean acquireLock() {
        return this.locked.compareAndSet(false, true);
    }

    public void releaseLock() {
        this.locked.set(false);
    }
}
