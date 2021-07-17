package it.unitn.ds1.project.message;

import java.io.Serializable;

public class WriteResultMessage implements Serializable {

    public final String transactionID;

    public WriteResultMessage(String transactionID){

        this.transactionID = transactionID;

    }

}
