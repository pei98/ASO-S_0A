package com.test.contrast.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import com.test.contrast.model.PacketIndex;
import com.test.contrast.service.mutithread.HashBuck;
import com.test.contrast.service.mutithread.Node;
import org.apache.commons.io.input.ReversedLinesFileReader;
import sun.security.krb5.internal.PAData;

import javax.sound.midi.Soundbank;
import javax.swing.plaf.multi.MultiTabbedPaneUI;
import java.io.*;
import java.nio.charset.Charset;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

public class MultiSta {
    public static void main(String[] args) {
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        List<String> indexpaths = new ArrayList<String>();
        //地面联测数据
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\Cut_01.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\Cut_02.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\Cut_03.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_hxi_T20200611_000102_SY_AA\\ASO-S_lev0A_hxi_T20200611_000102_SY_AA_sorted_01.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_hxi_T20200611_000102_SY_AA\\ASO-S_lev0A_hxi_T20200611_000102_SY_AA_sorted_02.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_hxi_T20200611_000102_SY_AA\\ASO-S_lev0A_hxi_T20200611_000102_SY_AA_sorted_03.index");


        //卫星数传数据--常规数据
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_fmg_T20221114_000529_MY_AA\\ASO-S_lev0A_fmg_T20221114_000529_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_fmg_T20221114_000530_MY_AA\\ASO-S_lev0A_fmg_T20221114_000530_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_fmg_T20221114_000532_KS_AA\\ASO-S_lev0A_fmg_T20221114_000532_KS_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_hxi_T20221114_000529_MY_AA\\ASO-S_lev0A_hxi_T20221114_000529_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_hxi_T20221114_000530_MY_AA\\ASO-S_lev0A_hxi_T20221114_000530_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_hxi_T20221114_000532_KS_AA\\ASO-S_lev0A_hxi_T20221114_000532_KS_AA.index");
//
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_wst_T20221023_000218_MY_AA\\ASO-S_lev0A_wst_T20221023_000218_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_wst_T20221024_000219_KS_AA\\ASO-S_lev0A_wst_T20221024_000219_KS_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_wst_T20221024_000220_KS_AA\\ASO-S_lev0A_wst_T20221024_000220_KS_AA.index");

//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_fmg_T20230307_002176_MY_AA\\ASO-S_lev0A_fmg_T20230307_002176_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_fmg_T20230308_002177_KS_AA\\ASO-S_lev0A_fmg_T20230308_002177_KS_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_fmg_T20230308_002178_KS_AA\\ASO-S_lev0A_fmg_T20230308_002178_KS_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_hxi_T20230307_002176_MY_AA\\ASO-S_lev0A_hxi_T20230307_002176_MY_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_hxi_T20230308_002177_KS_AA\\ASO-S_lev0A_hxi_T20230308_002177_KS_AA.index");
//        indexpaths.add("F:\\normal\\out\\ASO-S_lev0A_hxi_T20230308_002178_KS_AA\\ASO-S_lev0A_hxi_T20230308_002178_KS_AA.index");

//        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_hxi_T20221023_000218_MY_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_hxi_T20221024_000219_KS_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_hxi_T20221024_000220_KS_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1114\\ASO-S_lev0A_hxi_T20221114_000529_MY_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1114\\ASO-S_lev0A_hxi_T20221114_000530_MY_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1114\\ASO-S_lev0A_hxi_T20221114_000532_KS_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1114\\ASO-S_lev0A_fmg_T20221114_000529_MY_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1114\\ASO-S_lev0A_fmg_T20221114_000530_MY_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1114\\ASO-S_lev0A_fmg_T20221114_000532_KS_AA.index");
        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_wst_T20221023_000218_MY_AA.index");
        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_wst_T20221024_000219_KS_AA.index");
        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_wst_T20221024_000220_KS_AA.index");



        //卫星数传数据--轨间数据合并异常数804&805轨
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_sdi_T20221203_000804_MY_AA\\ASO-S_lev0A_sdi_T20221203_000804_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_sdi_T20221203_000805_MY_AA\\ASO-S_lev0A_sdi_T20221203_000805_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_fmg_T20221203_000804_MY_AA\\ASO-S_lev0A_fmg_T20221203_000804_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_fmg_T20221203_000805_MY_AA\\ASO-S_lev0A_fmg_T20221203_000805_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_hxi_T20221203_000804_MY_AA\\ASO-S_lev0A_hxi_T20221203_000804_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_hxi_T20221203_000805_MY_AA\\ASO-S_lev0A_hxi_T20221203_000805_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_wst_T20221203_000804_MY_AA\\ASO-S_lev0A_wst_T20221203_000804_MY_AA.index");
//        indexpaths.add("F:\\804and805\\out\\ASO-S_lev0A_wst_T20221203_000805_MY_AA\\ASO-S_lev0A_wst_T20221203_000805_MY_AA.index");


        //实现索引数据的合并及重排序、去重，不包括从dat文件中读取源包数据根据有序index文件重组
        //mergeMultiStations1(indexpaths);
//        System.out.println("---");
//        //采用索引片段切割的方式进行数据合并，减小待处理的数据量，使用的是原定的索引排序方式
//        ArrayList<PacketIndex> res = mergeWithCutIndex(indexpaths);
//        System.out.println("---");
//        //采用索引片段切割的方式进行数据合并，减小待处理的数据量，针对卫星校时问题导致的源包排序异常进行排序算法修正后的方法
        ArrayList<PacketIndex> resIndex = mergeWithCutQueue(indexpaths);
        //使用多线程的方式进行排序
        //List<PacketIndex> resMT = mergeWithMultiTread(indexpaths);

    }

    private static List<PacketIndex> mergeWithMultiTread(List<String> indexpaths){
        List<PacketIndex> indexlist = ReadAndSort.readIndex(indexpaths);
        System.out.println(indexlist.size());

        long s = System.currentTimeMillis();
        HashBuck hashBuck = new HashBuck();
        for (int i=0;i<indexlist.size();i++){
            PacketIndex p=indexlist.get(i);
            hashBuck.put(p.getTimeSec(),p);
        }
        long t1 = System.currentTimeMillis();
        System.out.println("hash:"+(t1-s)+"ms");
        List<List<PacketIndex>> indexlists=new ArrayList<>();
        System.out.println("hashBuck.getArray.length="+hashBuck.getArray().length);
        for (int i=0;i<hashBuck.getArray().length;i++){
            Node node = hashBuck.getNode(i);
            if (node!=null){
                MultiTread t = new MultiTread();
                t.setNode(node);
                FutureTask ft = new FutureTask<>(t);
                Thread thread=new Thread(ft);
                thread.start();
                try {
                    List<PacketIndex> l= (List<PacketIndex>) ft.get();
                    //不必使用其他工具进行转换，直接对Object进行强制类型转换即可
//                    Object l = ft.get();
//                    List<PacketIndex> ll = new ObjectMapper().convertValue(l, List.class);
                    indexlists.add(l);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        }
        System.out.println("indexlists.size="+indexlists.size());
        while (indexlists.size()>1){
            indexlists=mergeSort(indexlists);
        }

        long e = System.currentTimeMillis();
        System.out.println("sort:"+(e-s));
        return indexlists.get(0);
    }

    //使用归并排序完成hash分桶后各个桶的排序结果
    private static List<List<PacketIndex>> mergeSort(List<List<PacketIndex>> lists) {

        int size=lists.size();
        if (size==1){
            return lists;
        }

        List<List<PacketIndex>> res=new ArrayList<>();
        if (size%2==1){
            res.add(lists.get(size-1));
            size=size-1;
        }
        for (int i=0;i<size;i=i+2){
            List<PacketIndex> l1=lists.get(i);
            List<PacketIndex> l2=lists.get(i+1);
            List<PacketIndex> mergeTwoRes = merge(l1,l2);
            res.add(mergeTwoRes);
        }
        return res;


    }
    private static List<PacketIndex> merge(List<PacketIndex> l1, List<PacketIndex> l2){
        List<PacketIndex> res =new ArrayList<>();
        int i=0,j=0;
        while (i<l1.size() && j<l2.size()){
            PacketIndex o1=l1.get(i);
            PacketIndex o2=l2.get(j);
            if(o1.getTimeSec()>o2.getTimeSec()){
                res.add(o2);
                j++;
            }
            if(o1.getTimeSec()<o2.getTimeSec()){
                res.add(o1);
                i++;
            }
            if(o1.getTimeSec()==o2.getTimeSec()){
                if(Math.abs(o1.getPackCount()-o2.getPackCount())<0x2000){
                    if(o1.getPackCount()>o2.getPackCount()){
                        res.add(o2);
                        j++;
                    }
                    if(o1.getPackCount()<o2.getPackCount()){
                        res.add(o1);
                        i++;
                    }else{
                        res.add(o1);
                        i++;
                        j++;
                    }
                }
                if(Math.abs(o1.getPackCount()-o2.getPackCount())>=0x2000){
                    if(o1.getPackCount()>o2.getPackCount()){
                        res.add(o1);
                        i++;
                    }
                    if(o1.getPackCount()<o2.getPackCount()){
                        res.add(o2);
                        j++;
                    }else{
                        res.add(o2);
                        j++;
                        i++;
                    }

                }
            }

        }
        if (i==l1.size() && j<l2.size()){
            res.addAll(l2.subList(j,l2.size()));
        }
        if (j==l2.size() && i<l1.size()){
            res.addAll(l1.subList(i,l1.size()));
        }
        return res;
    }


    /*
    private static ConcurrentHashMap<PacketIndex, String> mergeWithMultiTread(List<String> indexpaths) {
        ConcurrentHashMap<PacketIndex, String> indexmap = ReadAndSort.readIndexWithCHsahM(indexpaths);
        System.out.println(indexmap.size());
        long s = System.currentTimeMillis();

        int block=indexmap.size()/5;
        MultiTread t = new MultiTread();
        t.setIndexmap(indexmap);
        for (int i=0;i<6;i++){

        }

        ConcurrentHashMap<PacketIndex, String> collect1 = indexmap.entrySet().stream().sorted(new Comparator<Map.Entry<PacketIndex, String>>() {
            @Override
            public int compare(Map.Entry<PacketIndex, String> o1, Map.Entry<PacketIndex, String> o2) {

                if(o1 == null && o2 == null) {
                    return 0;
                }
                if(o1 == null) {
                    return -1;
                }
                if(o2 == null) {
                    return 1;
                }
                PacketIndex p1=o1.getKey();
                PacketIndex p2=o2.getKey();
                if(p1.getTimeSec()>p2.getTimeSec()){
                    return 1;
                }
                if(p1.getTimeSec()<p2.getTimeSec()){
                    return -1;
                }
                if(p1.getTimeSec()==p2.getTimeSec()){
                    if(Math.abs(p1.getPackCount()-p2.getPackCount())<0x2000){
                        if(p1.getPackCount()>p2.getPackCount()){
                            return 1;
                        }
                        if(p1.getPackCount()<p2.getPackCount()){
                            return -1;
                        }
                        if(p1.getPackCount()==p2.getPackCount()){
                            return 0;
                        }
                    }
                    if(Math.abs(p1.getPackCount()-p2.getPackCount())>=0x2000){
                        if(p1.getPackCount()>p2.getPackCount()){
                            return -1;
                        }
                        if(p1.getPackCount()<p2.getPackCount()){
                            return 1;
                        }
                        if(p1.getPackCount()==p2.getPackCount()){
                            return 0;
                        }
                    }
                }
                return 1;
            }
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, ConcurrentHashMap::new));

        return collect1;
    }
    */

    private static ArrayList<PacketIndex> mergeWithCutQueue(List<String> indexpaths) {
        //List<PacketIndex> indexlist = ReadAndSort.readIndex(indexpaths);
        List<ArrayList<PacketIndex>> lists = ReadAndSort.readEveryIndex(indexpaths);
        //System.out.println(lists.get(0).size()+" "+lists.get(1).size());

        long s = System.currentTimeMillis();
        ArrayList<PacketIndex> res = new ArrayList<PacketIndex>();

        int num = lists.size();
        long nextTime = lists.get(1).get(0).getTimeSec();
        ArrayList<PacketIndex> lastList = lists.get(0);
        long lastTime = lastList.get(lastList.size() - 1).getTimeSec();
        long sign = 0;
        if (lastTime<nextTime){
            res.addAll(lists.get(0));
            System.out.println("无交叉");
        }else{
            for (int i=1;i<=num-1;i++){
                ArrayList<PacketIndex> list1 = lists.get(i - 1);
                ArrayList<PacketIndex> list2 = lists.get(i);
                int cut1 = list1.size()*9/10;
                int cut2 = list2.size()/10;
                if (i==1){
                    List<PacketIndex> index0 = list1.subList(0, cut1);
                    res.addAll(index0);

                }else{
                    List<PacketIndex> index0 = list1.subList(list1.size()/10, cut1);
                    res.addAll(index0);
                }
                System.out.println(res.size());

                List<PacketIndex> index1 = list1.subList(cut1, list1.size());
                List<PacketIndex> index2 = list2.subList(0, cut2);
                //System.out.println("去重前："+index1.size()+" "+index2.size()+" 去重前的总索引数："+index1.size()+index2.size());
                ArrayList<PacketIndex> combineList = combinePacketIndexList(new ArrayList<>(index1), new ArrayList<>(index2));
                //System.out.println("去重后："+combineList.size());
                res.addAll(combineList);
                //System.out.println(res.size());

                if (i==num-1){
                    List<PacketIndex> index3 = list2.subList(cut2, list2.size());
                    res.addAll(index3);
                }else{
                    //List<PacketIndex> list3 = lists.get(i+1);
                    List<PacketIndex> index3 = list2.subList(cut2, list2.size()*9/10);
                    res.addAll(index3);
                }

            }
        }
        long e = System.currentTimeMillis();
        System.out.println(res.size());
        System.out.println("total:"+(e-s));
        return res;
    }

    public static ArrayList<PacketIndex> combinePacketIndexList(ArrayList<PacketIndex> cur_l,ArrayList<PacketIndex> his_l){
        ArrayList<PacketIndex> reuslt_l=new ArrayList<>();
        int loc_cur=0;
        int loc_his=0;
        int cur_length=cur_l.size();
        int his_length=his_l.size();

        while(true){
            if(loc_cur>=cur_length){
                break;
            }
            if(loc_his>=his_length){
                break;
            }
            PacketIndex cur_index=cur_l.get(loc_cur);
            PacketIndex his_index=his_l.get(loc_his);

//            if(cur_index.time_ms/1000<his_index.time_ms/1000){
//                //如果当前时间在历史时间以前，那么将当前索引加入结果
//                reuslt_l.add(cur_index);
//                //当前索引指针后移
//                loc_cur++;
//            }else if (cur_index.time_ms/1000>his_index.time_ms/1000){
//                reuslt_l.add(his_index);
//                //如果历史数据在前，将历史数据写入到结果
//                loc_his++;
//            }else if(cur_index.time_ms/1000==his_index.time_ms/1000){
            if(cur_index.timeSec<his_index.timeSec){
                //如果当前时间在历史时间以前，那么将当前索引加入结果
                reuslt_l.add(cur_index);
                //当前索引指针后移
                loc_cur++;
            }else if (cur_index.timeSec>his_index.timeSec){
                reuslt_l.add(his_index);
                //如果历史数据在前，将历史数据写入到结果
                loc_his++;
            }else if(cur_index.timeSec==his_index.timeSec){
                //如果时间相等，则继续判断包计数
                int dist=Math.abs(cur_index.packCount-his_index.packCount);
                if (dist<0x2000){
                    if (cur_index.packCount<his_index.packCount){
                        //如果当前包计数小，则将其加入结果
                        reuslt_l.add(cur_index);
                        loc_cur++;
                    }else if (cur_index.getPackCount()>his_index.getPackCount()){
                        reuslt_l.add(his_index);
                        loc_his++;
                    }else{
                        //重复源包，记录历史的
                        reuslt_l.add(his_index);
                        //同时后移
                        loc_his++;
                        loc_cur++;
                    }
                }else {
                    if (cur_index.packCount>his_index.packCount){
                        //如果当前包计数大，则将其加入结果
                        reuslt_l.add(cur_index);
                        loc_cur++;
                    }else if (cur_index.packCount<his_index.packCount){
                        reuslt_l.add(his_index);
                        loc_his++;
                    }else{
                        //重复源包，记录历史的
                        reuslt_l.add(his_index);
                        //同时后移
                        loc_his++;
                        loc_cur++;
                    }
                }

            }
        }
        while(loc_cur<cur_length){
            //如果当前指针没有走完
            reuslt_l.add(cur_l.get(loc_cur));
            loc_cur++;
        }
        while (loc_his<his_length){
            reuslt_l.add(his_l.get(loc_his));
            loc_his++;
        }
        return  reuslt_l;
    }
    public static ArrayList<PacketIndex> mergeWithCutIndex(List<String> indexpaths) {
        //List<PacketIndex> indexlist = ReadAndSort.readIndex(indexpaths);
        List<ArrayList<PacketIndex>> lists = ReadAndSort.readEveryIndex(indexpaths);
        //System.out.println(lists.size());

        long s = System.currentTimeMillis();
        ArrayList<PacketIndex> res = new ArrayList<PacketIndex>();

        int num = lists.size();
        long nextTime = lists.get(1).get(0).getTimeSec();
        List<PacketIndex> lastList = lists.get(0);
        long lastTime = lastList.get(lastList.size() - 1).getTimeSec();
        long sign = 0;
        if (lastTime<nextTime){
            res.addAll(lists.get(0));
        }else{
            for (int i=1;i<=num-1;i++){
                List<PacketIndex> list1 = lists.get(i - 1);
                List<PacketIndex> list2 = lists.get(i);
                int cut1 = list1.size()*9/10;
                int cut2 = list2.size()/10;
                if (i==1){
                    List<PacketIndex> index0 = list1.subList(0, cut1);
                    res.addAll(index0);
                }else{
                    List<PacketIndex> index0 = list1.subList(list1.size()/10, cut1);
                    res.addAll(index0);
                }

                List<PacketIndex> index1 = list1.subList(cut1, list1.size());
                List<PacketIndex> index2 = list2.subList(0, cut2);
                index1.addAll(index2);
                System.out.println("去重前："+index1.size());
                RoarBitmapAPI.removeWithRoarBitmapAPI(index1);
                ReadAndSort.SortByIndexList(index1);
                System.out.println("去重后："+index1.size());
                res.addAll(index1);


                if (i==num-1){
                    List<PacketIndex> index3 = list2.subList(cut2, list2.size());
                    res.addAll(index3);
                }else{
                    //List<PacketIndex> list3 = lists.get(i+1);
                    List<PacketIndex> index3 = list2.subList(cut2, list2.size()*9/10);
                    res.addAll(index3);
                    System.out.println(index3.size());
                }



            }
        }
        System.out.println(res.size());

        long e = System.currentTimeMillis();
        //System.out.println("sort:"+(s-s)+" removeDup:"+(e-s)+" total:"+(e-s));
        System.out.println("sort:"+(e-s));
        return res;

    }

    //采用原方法实现数据合并及排序去重
    public static void mergeMultiStations1(List<String> indexpaths){
        List<PacketIndex> indexlist = ReadAndSort.readIndex(indexpaths);
        System.out.println(indexlist.size());
        long s = System.currentTimeMillis();
        ReadAndSort.SortByIndexList(indexlist);
        System.out.println(indexlist.get(0));
        System.out.println(indexlist.get(1));
        long s1 = System.currentTimeMillis();
        String newPath =indexpaths.get(0).replace(".index","_Combine.index");
        RemoveDuplicate.removeWithOld(indexlist,newPath);

        long e = System.currentTimeMillis();
        System.out.println("sort:"+(s1-s)+" removeDup:"+(e-s1)+" total:"+(e-s));
    }

    public static void mergeMultiStations2(List<String> indexpaths) throws IOException {
        String path1 = indexpaths.get(0);
        List<PacketIndex> newIndexList = new ArrayList<PacketIndex>();
        //将多个台站的数据合并问题拆分为相邻两轨的两两合并问题
        for (int i=1;i<indexpaths.size();i++){
            String path2 = indexpaths.get(i);
            mergeTwoStations(path1, path2,newIndexList);
            path1 = path2;
        }
    }

    /*
    *将两轨数据合并时进行索引切割，
    * 前一轨数据切割点的选择是读取最后的time，并向前读取POSSIBLE_COINCIDENCE = 10个时间码的索引片段作为待合并的索引数据；
    * 后一轨数据类似,读取第一个time，并向后读取POSSIBLE_COINCIDENCE = 10个时间码的索引片段作为待合并的索引数据
    * POSSIBLE_COINCIDENCE可调整*/
    public static void mergeTwoStations(String path1,String path2,List<PacketIndex> newIndexList) throws IOException {
        File lastFile = new File(path1);
        int POSSIBLE_COINCIDENCE = 10;
        List<PacketIndex> psbIndexList = new ArrayList<PacketIndex>();

        //读前一个文件的后半部分内容
        ReversedLinesFileReader reversedLinesReader = new ReversedLinesFileReader(lastFile, Charset.defaultCharset());
        String line = reversedLinesReader.readLine();
        PacketIndex index = ReadAndSort.getIndexObj(line);
        psbIndexList.add(index);
        long lastTime = index.getTimeSec();
        int countTime=1;
        while(countTime<POSSIBLE_COINCIDENCE){
            line = reversedLinesReader.readLine();
            index = ReadAndSort.getIndexObj(line);
            psbIndexList.add(index);
            if(index.getTimeSec() == lastTime){
                continue;
            }else{
                countTime++;
                lastTime = index.getTimeSec();
            }
        }

        //读下一个文件的前一部分
        File currentFile = new File(path2);
        FileInputStream inputStream = new FileInputStream(currentFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        line = reversedLinesReader.readLine();
        index = ReadAndSort.getIndexObj(line);
        psbIndexList.add(index);
        lastTime = index.getTimeSec();
        countTime=1;
        while (countTime < POSSIBLE_COINCIDENCE) {
            line = reversedLinesReader.readLine();
            index = ReadAndSort.getIndexObj(line);
            psbIndexList.add(index);
            if(index.getTimeSec() == lastTime){
                continue;
            }else{
                countTime++;
                lastTime = index.getTimeSec();
            }
        }
        ReadAndSort.SortByIndexList(psbIndexList);
        RoarBitmapAPI.removeWithRoarBitmapAPI(psbIndexList);
        newIndexList.addAll(psbIndexList);

    }
}
