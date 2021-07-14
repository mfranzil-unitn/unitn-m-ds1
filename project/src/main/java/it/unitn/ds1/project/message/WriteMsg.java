package it.unitn.ds1.project.message;

import java.io.Serializable;

public class WriteMsg implements Serializable {
    public final Integer clientId;
    public final Integer key; // the key of the value to write
    public final Integer value; // the new value to write

    public WriteMsg(int clientId, int key, int value) {
        this.clientId = clientId;
        this.key = key;
        this.value = value;
    }
}
