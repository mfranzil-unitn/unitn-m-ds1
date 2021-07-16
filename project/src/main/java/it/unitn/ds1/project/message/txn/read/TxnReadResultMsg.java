package it.unitn.ds1.project.message.txn.read;


import java.io.Serializable;

// reply from the coordinator when requested a READ on a given key
public class TxnReadResultMsg implements Serializable {
    public final Integer key; // the key associated to the requested item
    public final Integer value; // the value found in the data store for that item

    public TxnReadResultMsg(int key, int value) {
        this.key = key;
        this.value = value;
    }
}

