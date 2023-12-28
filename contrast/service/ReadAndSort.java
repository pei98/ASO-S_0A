package com.test.contrast.service;

import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import com.test.contrast.model.PacketIndex;
import com.test.simulatie.PhotoPacket;
import org.json4s.JsonUtil;
import scala.util.control.Exception;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ReadAndSort {
    public static List<PacketIndex> readIndex(List<String> indexpaths){
        List<PacketIndex> indexList = new ArrayList<PacketIndex>();
        for(String path:indexpaths){
            File index = new File(path);
            if(!index.exists()){
                System.out.println("索引文件获取失败:"+path);
            }
            List<PacketIndex> list=getIndexList(path);
            indexList.addAll(list);
        }
        return indexList;
    }

    public static ConcurrentHashMap<PacketIndex,String> readIndexWithCHsahM(List<String> indexpaths){

        int initialCapcity=1<<16;
        ConcurrentHashMap<PacketIndex,String> map = new ConcurrentHashMap<>(initialCapcity);
        for(String path:indexpaths){
            File index = new File(path);
            if(!index.exists()){
                System.out.println("索引文件获取失败:"+path);
            }
            ConcurrentHashMap<PacketIndex, String> m=getIndexList(path,map.size());
            map.putAll(m);
        }
        return map;
    }

    public static List<ArrayList<PacketIndex>> readEveryIndex(List<String> indexpaths){
        List<ArrayList<PacketIndex>> res = new ArrayList<ArrayList<PacketIndex>>();
        for(String path:indexpaths){
            File index = new File(path);
            if(!index.exists()){
                System.out.println("索引文件获取失败:"+path);
            }
            ArrayList<PacketIndex> list=getIndexList(path);
            System.out.println(list.size());
            res.add(list);
        }
        return res;
    }


    public static ArrayList<PacketIndex> getIndexList(String indexpath){
        BufferedReader reader =null;
        try {
            reader=new BufferedReader(new FileReader(indexpath));
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }
        ArrayList<PacketIndex> indexList=new ArrayList<>();
        String currentLine="";
        PacketIndex currentIndexObj=null;
        while(true){
            try {
                if((currentLine=reader.readLine())==null){
                    break;
                }
                if(currentLine.equals("")){
                    continue;
                }
                if((currentIndexObj=getIndexObj(currentLine))==null){
                    continue;
                }else{
                    indexList.add(currentIndexObj);
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
        return indexList;
    }

    public static ConcurrentHashMap<PacketIndex,String> getIndexList(String indexpath,int start){
        BufferedReader reader =null;
        try {
            reader=new BufferedReader(new FileReader(indexpath));
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }
        int i=start;
        int initialCapacity = 1<<16;
        ConcurrentHashMap<PacketIndex,String> indexmap=new ConcurrentHashMap<>(initialCapacity);
        String currentLine="";
        PacketIndex currentIndexObj=null;
        while(true){
            try {
                if((currentLine=reader.readLine())==null){
                    break;
                }
                if(currentLine.equals("")){
                    continue;
                }
                if((currentIndexObj=getIndexObj(currentLine))==null){
                    continue;
                }else{
                    indexmap.put(currentIndexObj,i+"");
                    i++;
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
        return indexmap;
    }

    public static PacketIndex getIndexObj(String str){
        if(str.equals("")){
            return null;
        }
        //System.out.println(str);
        String arr[]=str.split(",");
        if(arr.length<3){
            System.out.println("该条索引信息异常："+str );
            return null;
        }
        long timesec=Long.parseLong(arr[0]);
        int count=Integer.parseInt(arr[1]);
        long loc=Long.parseLong(arr[2]);
        PacketIndex index=new PacketIndex();
        index.timeSec=timesec;
        index.packCount=count;
        index.location=loc;
        return index;
    }


    public static  void SortByIndexList(List<PacketIndex> indexList){

        if(indexList==null){
            return;
        }
//        try {
            Collections.sort(indexList, new Comparator<PacketIndex>() {
                @Override
                public int compare(PacketIndex o1, PacketIndex o2) {
                    //System.out.println(o1.timeSec+","+o1.packCount+";   "+o2.timeSec+","+o2.packCount);
                    if(o1 == null && o2 == null) {
                        return 0;
                    }
                    if(o1 == null) {
                        return -1;
                    }
                    if(o2 == null) {
                        return 1;
                    }
                    if(o1.getTimeSec()>o2.getTimeSec()){
                        return 1;
                    }
                    if(o1.getTimeSec()<o2.getTimeSec()){
                        return -1;
                    }
                    if(o1.getTimeSec()==o2.getTimeSec()){
                        if(Math.abs(o1.getPackCount()-o2.getPackCount())<0x2000){
                            if(o1.getPackCount()>o2.getPackCount()){
                                return 1;
                            }
                            if(o1.getPackCount()<o2.getPackCount()){
                                return -1;
                            }
                            if(o1.getPackCount()==o2.getPackCount()){
                                return 0;
                            }
                        }
                        if(Math.abs(o1.getPackCount()-o2.getPackCount())>=0x2000){
                            if(o1.getPackCount()>o2.getPackCount()){
                                return -1;
                            }
                            if(o1.getPackCount()<o2.getPackCount()){
                                return 1;
                            }
                            if(o1.getPackCount()==o2.getPackCount()){
                                return 0;
                            }
                        }
                    }
                    return 1;
                }

            });
//        }catch (IllegalArgumentException e){
//            System.out.println(e.getMessage());
//        }

    }

    public static void Sortdat(List<PacketIndex> indexList,String datpath,String apid) throws IOException {
        RandomAccessFile reader=new RandomAccessFile(datpath,"r");
        String sorted_dat=datpath.replace(".dat","_"+apid+"_sorted.dat");
        BufferedOutputStream outputStream= new BufferedOutputStream(new FileOutputStream(sorted_dat));
        PacketIndex index;
        byte[] bytes=new byte[1024];
        long lineLoc=0;
        int sum=0;
        for(int i=0;i<indexList.size();i++){
            index=indexList.get(i);
            lineLoc=index.getLocation();
            reader.seek(lineLoc);
            reader.read(bytes);
            outputStream.write(bytes);
        }
        reader.close();
        outputStream.close();
    }
}
