package it.unitn.ds1.project.message.dss.decision;

import java.io.Serializable;

public class DecisionResponse implements Serializable {
    public final Decision decision;
    public final String transactionID;

    public DecisionResponse(String transactionID, Decision d) {
        this.transactionID = transactionID;
        this.decision = d;
    }
}
