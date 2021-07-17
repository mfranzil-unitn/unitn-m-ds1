package it.unitn.ds1.project.message.dss.decision;

import it.unitn.ds1.project.message.dss.DSSMessage;

public class DSSDecisionRequest extends DSSMessage {

    public DSSDecisionRequest(String transactionID) {
        super(transactionID);
    }
}
