package it.unitn.ds1.project.message.txn.begin;

import java.io.Serializable;

// message the client sends to a coordinator to begin the TXN
public class TxnBeginMsg implements Serializable {
    public final Integer clientId;

    public TxnBeginMsg(int clientId) {
        this.clientId = clientId;
    }
}
