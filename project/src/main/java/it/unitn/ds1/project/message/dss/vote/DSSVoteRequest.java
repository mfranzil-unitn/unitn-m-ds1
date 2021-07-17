package it.unitn.ds1.project.message.dss.vote;

import it.unitn.ds1.project.message.dss.DSSMessage;

public class DSSVoteRequest extends DSSMessage {

    public DSSVoteRequest(String transactionID) {
        super(transactionID);
    }
}
