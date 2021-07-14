package it.unitn.ds1.project.message;

import java.io.Serializable;

public class TxnReadMsg implements Serializable {

    public final String transactionID;
    public final Integer key;

    public TxnReadMsg(String transactionID, Integer key){

        this.transactionID = transactionID;
        this.key = key;

    }

}
