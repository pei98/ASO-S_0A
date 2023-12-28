package com.test.contrast.service.mutithread;

import com.test.contrast.model.PacketIndex;

public class Node {
    public long key;
    public PacketIndex value;
    public Node next;

    public long getKey() {
        return key;
    }

    public PacketIndex getValue() {
        return value;
    }

    public Node getNext() {
        return next;
    }

    public Node(long data,PacketIndex value) {
        this.key = data;
        this.value = value;
        this.next=null;
    }
}
