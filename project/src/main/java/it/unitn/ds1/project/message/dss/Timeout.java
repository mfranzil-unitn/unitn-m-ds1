package it.unitn.ds1.project.message.dss;

import java.io.Serializable;

public class Timeout implements Serializable {
    public final String transactionID;

    public Timeout(String transactionID) {
        this.transactionID = transactionID;
    }
}
