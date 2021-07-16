package it.unitn.ds1.project.message.dss.read;

import java.io.Serializable;

public class DSSReadRequestMsg implements Serializable {

    public final String transactionID;
    public final Integer key;

    public DSSReadRequestMsg(String transactionID, Integer key) {

        this.transactionID = transactionID;
        this.key = key;

    }

}
