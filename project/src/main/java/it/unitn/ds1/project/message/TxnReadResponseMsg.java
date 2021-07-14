package it.unitn.ds1.project.message;

import java.io.Serializable;

public class TxnReadResponseMsg implements Serializable {

    public final String transactionID;
    public final Integer value;

    public TxnReadResponseMsg(String transactionID, Integer value){

        this.transactionID = transactionID;
        this.value = value;

    }

}
