package it.unitn.ds1.project.message.dss.write;

import java.io.Serializable;

public class DSSWriteRequestMsg implements Serializable {

    public final String transactionID;
    public final Integer key;
    public final Integer value;

    public DSSWriteRequestMsg(String tID, Integer key, Integer value) {
        this.transactionID = tID;
        this.key = key;
        this.value = value;
    }

}
