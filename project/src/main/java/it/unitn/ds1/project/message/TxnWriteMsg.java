package it.unitn.ds1.project.message;

import java.io.Serializable;

public class TxnWriteMsg implements Serializable {

    public final String transactionID;
    public final Integer key;
    public final Integer value;

    public TxnWriteMsg(String tID, Integer key, Integer value){

        this.transactionID = tID;
        this.key = key;
        this.value = value;

    }

}
