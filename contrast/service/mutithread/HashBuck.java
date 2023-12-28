package com.test.contrast.service.mutithread;

import com.test.contrast.model.PacketIndex;

public class HashBuck {

    public Node[] getArray() {
        return array;
    }

    public Node getNode(int i){
        return array[i];
    }

    public void setArray(Node[] array) {
        this.array = array;
    }

    public int getUsedSize() {
        return usedSize;
    }

    public void setUsedSize(int usedSize) {
        this.usedSize = usedSize;
    }

    public Node[] array;
    public int usedSize; //总长度；
    public HashBuck() {
        this.array = new Node[16];
        this.usedSize = 0;
    }
    public void put(long key,PacketIndex value){

        //找到位置
        int index = (int)key % this.array.length;

        //todo：由于不准备在hash过程中实现去重操作，因此不会根据时间码判断是否已有key存在于hash桶中，而是直接将PacketIndex对象插入到对应的链表中
//        //遍历这个位置下的链表；看看是否存在这个key；
//        for (Node cur = array[index];cur != null;cur = cur.next) {
//            if(cur.key == key){
//                cur.value = value;
//                return;
//            }
//        }

        //该链表没有和当前key值一样的元素；jdk1.8使用尾插，这里使用头插法；
        Node node = new Node(key,value);
        node.next = array[index];
        array[index] = node;
        this.usedSize++;
        if(loadFactor() > 1.25){ //
            resize();
        }
    }
    public float loadFactor(){ //计算负载因子；
        return this.usedSize * 1.0f / this.array.length;
    }
    public void resize() { //扩容，然后重新哈希；
        Node[] newArray = new Node[this.array.length *2];
        for (int i = 0; i < this.array.length ; i++) {
            Node curNext = null;
            for (Node cur = array[i]; cur != null; cur = curNext) {
                curNext = cur.next;
                //array[i]是一个链表；cur为头节点;
                int index = (int)cur.key % newArray.length;
                cur.next = newArray[index]; //
                newArray[index] = cur;
            }
        }
        this.array = newArray;
    }
    public PacketIndex getValue(long key) {
        int index = (int)key % this.array.length;
        Node cur = this.array[index];
        for(; cur!= null; cur = cur.next) {
            if(cur.key == key) return cur.value;
        }
        return null;
    }


}
