package it.unitn.ds1.project.model;

import akka.actor.ActorRef;

import java.util.concurrent.atomic.AtomicBoolean;

public class DataItem {
    private Integer value;
    private Integer version;
    private boolean touched;

    private ActorRef locker;
    private final AtomicBoolean locked = new AtomicBoolean(false);

    public DataItem(Integer value, Integer version) {
        this.value = value;
        this.version = version;
    }

    public DataItem(DataItem other) {
        this.version = other.version;
        this.touched = false;
        this.value = other.value;
    }

    public Integer getValue() {
        return this.value;
    }

    public Integer getVersion() {
        return this.version;
    }

    public void setValue(Integer value, ActorRef locker) {
        if (locker == null) {
        } else if (locker != this.locker) throw new RuntimeException(
                "Wrong locker! " + locker + " vs " + this.locker);
        this.value = value;
    }

    public void setVersion(Integer version, ActorRef locker) {
        if (locker == null) {
        } else if (locker != this.locker) throw new RuntimeException("Wrong locker!");
        this.version = version;
    }

    public void incrementVersion(ActorRef locker) {
        if (locker == null) {
        } else if (locker != this.locker) throw new RuntimeException("Wrong locker!");
        if (!touched) {
            touched = true;
            this.version++;
        }
    }

    public boolean acquireLock(ActorRef locker) {
        boolean res = this.locked.compareAndSet(false, true);
        if (res) this.locker = locker;
        return res;
    }

    public void releaseLock() {
        this.locker = null;
        this.locked.set(false);
    }

    @Override
    public String toString() {
        return "DataItem{" +
                "value=" + value +
                ", version=" + version +
                ", touched=" + touched +
                ", locked=" + locked +
                '}';
    }
}
