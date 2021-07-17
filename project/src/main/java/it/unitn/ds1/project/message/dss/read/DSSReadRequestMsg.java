package it.unitn.ds1.project.message.dss.read;

import it.unitn.ds1.project.message.dss.DSSMessage;

public class DSSReadRequestMsg extends DSSMessage {
    public final Integer key;

    public DSSReadRequestMsg(String transactionID, Integer key) {
        super(transactionID);
        this.key = key;
    }
}
