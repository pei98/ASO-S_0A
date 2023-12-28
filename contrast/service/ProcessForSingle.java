package com.test.contrast.service;

import com.sun.corba.se.impl.interceptors.PICurrent;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import com.test.contrast.model.PacketIndex;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessForSingle {
    private static int winSize=7;
    private static String gps = "F:\\ASO-S\\data\\index\\mock\\gpsoff\\GPS.dat";
    public static void main(String[] args) throws IOException {
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        List<String> indexpaths = new ArrayList<String>();
        indexpaths.add("F:\\ASO-S\\data\\index\\mock\\源包乱序\\ASO-S_lev0A_hxi_T20221023_000218_MY_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_hxi_T20221024_000219_KS_AA.index");
//        indexpaths.add("F:\\ASO-S\\data\\index\\1023-1024\\ASO-S_lev0A_hxi_T20221024_000220_KS_AA.index");

        for(String path:indexpaths){
            File index = new File(path);
            if(!index.exists()){
                System.out.println("索引文件获取失败:"+path);
            }
            List<String> error = new ArrayList<>();
            List<String> loss = new ArrayList<>();
            List<PacketIndex> res=processSingle(path,error,loss);

            String outpath = path.replace(".index","_sorted.index");
            BufferedWriter writer=null;
            writer=new BufferedWriter(new FileWriter(outpath));
            for (PacketIndex i:res){
                writer.write(i.getTimeSec()+","+i.getPackCount()+","+i.getLocation()+"\n");
            }
            writer.flush();
            writer.close();

        }


    }

    private static List<PacketIndex> processSingle(String path,List<String> error,List<String> loss) {

        String bpPath = path.replace(".index","_bp.index");
        List<PacketIndex> list=ReadAndSort.getIndexList(path);
        List<PacketIndex> res = new ArrayList<PacketIndex>();
        res.add(list.get(0));
        //test:只读取20000条索引
        //for (int i=1;i<20000;i++){
        for (int i=1;i<list.size();i++){
            System.out.println(i);
            PacketIndex lastIndex = res.get(res.size()-1);
            PacketIndex curIndex = list.get(i);
            long lTime = lastIndex.getTimeSec();
            long cTime = curIndex.getTimeSec();
            int lCount = lastIndex.getPackCount();
            int cCount = curIndex.getPackCount();
            if (lCount<cCount || Math.abs(lCount-cCount)>16375){
                if (lTime<=cTime && (cTime-lTime)<5){
                    //连续
                    res.add(curIndex);
                }else{
                    //包计数连续，时间码不连续，进一步读取GPS非定位状态
                    //GPS非定位
                    List<Long> gpsoff= readGPS(gps);
                    if (gpsoff.contains(cTime)){
                        curIndex.setTimeSec(lastIndex.getTimeSec());
                        res.add(curIndex);
                    }else{
                        //GPS定位
                        long interval = Math.abs(cTime-lTime);
                        if (interval<5 && interval>0){
                            //出现了星上校时导致时间码异常
                            res.add(curIndex);
                        }else{
//                            if (list.get(i+1).getTimeSec()>=lTime){
//                                curIndex.setTimeSec(lTime);
//                                res.add(curIndex);
//                            }else{
                                System.out.println("GPS定位且时间差距大于5，"+cCount);
                                error.add(curIndex.getIndexLine()+"$time exception--GPSon\n");
//                            }

                        }
                    }

                }
            }else{
                //包计数不连续
                List<PacketIndex> window = new ArrayList<>();
                //以i为中心设置窗口
//                int half = winSize/2;
//                if (res.size()>half){
//                    window.addAll(res.subList(res.size()-half,res.size()));
//                    window.add(curIndex);
//                    if ((i+half)<list.size()){
//                        window.addAll(list.subList(i+1,i+1+half));
//                    }else{
//                        window.addAll(list.subList(i+1,list.size()));
//                    }
//
//                }else{
//                    window.addAll(res);
//                    window.add(curIndex);
//                    if ((i+half)<list.size()){
//                        window.addAll(list.subList(i+1,i+1+half));
//                    }else{
//                        window.addAll(list.subList(i+1,list.size()));
//                    }
//                }
                //窗口部分已确定
//                ReadAndSort.SortByIndexList(window);
//                if ((window.get(half).getPackCount()==window.get(half+1).getPackCount()-1)||(window.get(half).getPackCount()==65533 && window.get(half+1).getPackCount()==0)){
//                    for (int j=i;j<i+half+1;j++){
//                        list.set(j,window.get(j-i));
//                    }
//                    i--;
//                }else{
//                    for (int j=lCount;j<cCount;j++){
//                        loss.add(lastIndex.getIndexLine()+"-"+curIndex.getIndexLine());
//                    }
//                }

                //从i-1至i+size-1的窗口
//                window.add(lastIndex);
//                window.add(curIndex);
//                if (i+winSize-1<list.size()){
//                    for (int j=i+1;j<i+winSize-1;j++){
//                        window.add(list.get(j));
//                    }
//                }else{
//                    window.addAll(list.subList(i+1,list.size()));
//                }
//                ReadAndSort.SortByIndexList(window);
//                if (window.get(0).getPackCount()<window.get(1).getPackCount()){
//                    for (int j=i;j<i+winSize-1;j++){
//                        list.set(j,window.get(j-i));
//                    }
//                    res.add(list.get(i));
//                }else {
//                    List<PacketIndex> bplist=ReadAndSort.getIndexList(bpPath);
//                    PacketIndex bpIndex = queryInBackup(bplist,lastIndex);
//                    if (bpIndex !=null){
//                        //list.add(i,bpIndex);
//                        res.add(bpIndex);
//                    }else{
//                        res.add(curIndex);
//                        loss.add("from "+lastIndex.getIndexLine()+"to "+curIndex.getIndexLine());
//                    }
//                }

                //从res中的后winsize-1个索引作为窗口，因此将res中的部分索引需要重排序
                if (res.size()>winSize){
                    window.addAll(res.subList(res.size()-winSize+1,res.size()));
//                    for (int j=res.size()-winSize+1;j<res.size();j++){
//                        window.add(res.get(j));
//                    }
                }else{
                    window.addAll(res);
                }
                window.add(curIndex);
                ReadAndSort.SortByIndexList(window);
                if (window.get(winSize-2).getPackCount()<window.get(winSize-1).getPackCount()){
                    int k=0;
                    for (int j=res.size()-winSize+1;j<res.size();j++){
                        res.set(j,window.get(k));
                        k++;
                    }
                    //res.add(list.get(i));
                }else {
                    List<PacketIndex> bplist=ReadAndSort.getIndexList(bpPath);
                    PacketIndex bpIndex = queryInBackup(bplist,lastIndex);
                    if (bpIndex !=null){
                        //list.add(i,bpIndex);
                        res.add(bpIndex);
                    }else{
                        res.add(curIndex);
                        loss.add("from "+lastIndex.getIndexLine()+"to "+curIndex.getIndexLine());
                    }
                }







            }
        }
//        System.out.println("error information:-----------------");
//        for (int i=0;i<error.size();i++){
//            System.out.println(error.get(i));
//        }
//        System.out.println("loss information:-----------------");
//        for (int i=0;i<loss.size();i++){
//            System.out.println(loss.get(i));
//        }
        System.out.println("时间码跳变"+error.size());
        return res;

    }

    private static List<Long> readGPS(String gps) {
        BufferedReader reader =null;
        try {
            reader=new BufferedReader(new FileReader(gps));
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }
        List<Long> gpsoff = new ArrayList<>();
        String line = "";
        while(true){
            try {
                if((line=reader.readLine())==null){
                    break;
                }
                if(line.equals("")){
                    continue;
                }
                Long t = Long.parseLong(line);
                if(t==null){
                    continue;
                }else{
                    gpsoff.add(t);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return gpsoff;
    }

    private static PacketIndex queryInBackup(List<PacketIndex> bplist,PacketIndex lastIndex) {
        int lastloc = bplist.indexOf(lastIndex);
        if (lastloc>=0){
            PacketIndex bpindex = bplist.get(lastloc+1);
            if (bpindex.getTimeSec()>=lastIndex.getTimeSec()){
                if (bpindex.getPackCount()==lastIndex.getPackCount()+1){
                    return bpindex;
                }
            }
        }
        return null;

    }
    private static void queryInBackup(List<PacketIndex> list,List<PacketIndex> bplist,List<String> error) {
        int j=0;
        for (int i=0;i<error.size();i++){
            String e = error.get(i);
            PacketIndex errObj = ReadAndSort.getIndexObj(e);
            long t = errObj.getTimeSec();
            int c = errObj.getPackCount();
            while (j<list.size()){
                PacketIndex tmp = list.get(i);
                if (tmp.getTimeSec()==t && tmp.getPackCount()==c){
                    tmp.setBpflag(true);
                    list.set(i,tmp);
                    break;
                }
            }
        }

    }
}
