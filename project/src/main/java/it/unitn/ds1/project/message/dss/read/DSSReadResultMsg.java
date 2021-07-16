package it.unitn.ds1.project.message.dss.read;

import java.io.Serializable;

public class DSSReadResultMsg implements Serializable {

    public final String transactionID;
    public final Integer value;

    public DSSReadResultMsg(String transactionID, Integer value) {

        this.transactionID = transactionID;
        this.value = value;

    }

}
