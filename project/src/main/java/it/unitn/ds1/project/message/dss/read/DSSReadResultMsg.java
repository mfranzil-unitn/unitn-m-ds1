package it.unitn.ds1.project.message.dss.read;

import it.unitn.ds1.project.message.dss.DSSMessage;

public class DSSReadResultMsg extends DSSMessage {
    public final Integer key;
    public final Integer value;

    public DSSReadResultMsg(String transactionID, Integer key, Integer value) {
        super(transactionID);
        this.key = key;
        this.value = value;
    }
}
