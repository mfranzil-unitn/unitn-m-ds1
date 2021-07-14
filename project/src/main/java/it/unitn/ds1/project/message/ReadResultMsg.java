package it.unitn.ds1.project.message;


import java.io.Serializable;

// reply from the coordinator when requested a READ on a given key
public class ReadResultMsg implements Serializable {
    public final Integer key; // the key associated to the requested item
    public final Integer value; // the value found in the data store for that item

    public ReadResultMsg(int key, int value) {
        this.key = key;
        this.value = value;
    }
}

