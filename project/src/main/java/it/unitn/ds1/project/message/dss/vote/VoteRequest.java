package it.unitn.ds1.project.message.dss.vote;

import java.io.Serializable;

public class VoteRequest implements Serializable {

    public final String transactionID;

    public VoteRequest(String tID) {
        this.transactionID = tID;
    }

}
