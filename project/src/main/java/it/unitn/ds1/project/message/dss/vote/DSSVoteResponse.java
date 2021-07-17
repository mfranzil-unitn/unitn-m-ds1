package it.unitn.ds1.project.message.dss.vote;

import it.unitn.ds1.project.message.dss.DSSMessage;

public class DSSVoteResponse extends DSSMessage {
    public final DSSVote vote;

    public DSSVoteResponse(String transactionID, DSSVote v) {
        super(transactionID);
        vote = v;
    }
}
