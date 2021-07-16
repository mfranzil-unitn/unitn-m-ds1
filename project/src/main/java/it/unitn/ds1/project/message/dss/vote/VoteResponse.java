package it.unitn.ds1.project.message.dss.vote;

import java.io.Serializable;

public class VoteResponse implements Serializable {
    public final String transactionID;
    public final Vote vote;

    public VoteResponse(String transactionID, Vote v) {
        this.transactionID = transactionID;
        vote = v;
    }
}
