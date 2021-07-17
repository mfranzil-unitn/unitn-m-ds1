package it.unitn.ds1.project.message.dss;

import java.io.Serializable;
import java.lang.reflect.MalformedParametersException;

public abstract class DSSMessage implements Serializable {

    public final String transactionID;

    protected DSSMessage(String transactionID) {
        if (transactionID == null) {
            throw new MalformedParametersException("Cannot have transactionID as null!");
        }
        this.transactionID = transactionID;
    }
}
