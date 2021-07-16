package it.unitn.ds1.project.message.dss.decision;

import java.io.Serializable;

public class DecisionRequest implements Serializable {
    public final String transactionID;

    public DecisionRequest(String transactionID) {
        this.transactionID = transactionID;
    }

}
