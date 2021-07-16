package it.unitn.ds1.project.message.dss.write;

import java.io.Serializable;

public class DSSWriteResultMsg implements Serializable {

    public final String transactionID;

    public DSSWriteResultMsg(String transactionID){

        this.transactionID = transactionID;

    }

}
