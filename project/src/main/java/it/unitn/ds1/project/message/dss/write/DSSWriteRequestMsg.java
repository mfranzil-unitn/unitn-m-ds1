package it.unitn.ds1.project.message.dss.write;

import it.unitn.ds1.project.message.dss.DSSMessage;

import java.io.Serializable;

public class DSSWriteRequestMsg extends DSSMessage {

    public final Integer key;
    public final Integer value;

    public DSSWriteRequestMsg(String transactionID, Integer key, Integer value) {
        super(transactionID);
        this.key = key;
        this.value = value;
    }

}
