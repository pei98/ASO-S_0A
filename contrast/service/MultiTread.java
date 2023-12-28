package com.test.contrast.service;

import com.test.contrast.model.PacketIndex;
import com.test.contrast.service.mutithread.HashBuck;
import com.test.contrast.service.mutithread.Node;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MultiTread implements Callable {

    public void setNode(Node node) {
        this.node = node;
    }

    private Node node;


    @Override
    public Object call() throws Exception {
        List<PacketIndex> list=new ArrayList<>();
        while (node!=null){
            PacketIndex index = node.getValue();
            list.add(index);
            node=node.getNext();
        }

        ReadAndSort.SortByIndexList(list);
        return list;
    }
}
