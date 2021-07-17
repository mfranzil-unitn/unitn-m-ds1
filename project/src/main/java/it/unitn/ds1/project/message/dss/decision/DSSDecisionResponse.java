package it.unitn.ds1.project.message.dss.decision;

import it.unitn.ds1.project.message.dss.DSSMessage;

public class DSSDecisionResponse extends DSSMessage {
    public final DSSDecision decision;

    public DSSDecisionResponse(String transactionID, DSSDecision d) {
        super(transactionID);
        this.decision = d;
    }
}
