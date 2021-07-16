package it.unitn.ds1.project.message.txn.end;

import java.io.Serializable;

// message the client sends to a coordinator to end the TXN;
// it may ask for commit (with probability COMMIT_PROBABILITY), or abort
public class TxnEndMsg implements Serializable {
    public final Integer clientId;
    public final Boolean commit; // if false, the transaction should abort

    public TxnEndMsg(int clientId, boolean commit) {
        this.clientId = clientId;
        this.commit = commit;
    }
}
