package com.test.contrast.service;


import com.test.contrast.model.PacketIndex;
import org.roaringbitmap.*;
import org.roaringbitmap.insights.BitmapAnalyser;
import org.roaringbitmap.insights.BitmapStatistics;

import java.io.*;
import java.util.*;

import static com.test.contrast.service.ReadAndSort.SortByIndexList;
import static com.test.contrast.service.RoarBitmapAPI.removeWithRoarBitmapAPI;

public class RemoveDuplicate {
    public static void main(String[] args) throws IOException {

        List<String> indexpaths = new ArrayList<String>();
        //index文件总行数244274
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\Cut_01.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\Cut_02.index");
//        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\Cut_03.index");

        indexpaths.add("E:\\data\\ASO-S\\index\\guidang\\16_110010_0909.index");
        indexpaths.add("E:\\data\\ASO-S\\index\\new\\100101_0A0A.index");
        indexpaths.add("E:\\data\\ASO-S\\index\\new\\110010_094F.index");
        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen01.index");

//        indexpaths.add("E:\\data\\ASO-S\\index\\guidang\\15_100101_0A0A.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\guidang\\15_110010_0909.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\guidang\\16_100101_0A0A.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\guidang\\16_110010_0909.index");
//
//        ///////////////indexpaths.add("E:\\data\\ASO-S\\index\\new\\011001_0A0A.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\100101_0A0A.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\100101_095C.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\110010_0C0B.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\110010_091A.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\110010_093F.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\110010_094F.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\new\\110010_0909.index");

//        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen01.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen02.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen03.index");
//        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen04.index");


        for (String indexpath:indexpaths){
            System.out.print(indexpath);
            List<PacketIndex> indexList = ReadAndSort.getIndexList(indexpath);

            long s1 = System.currentTimeMillis();
            ReadAndSort.SortByIndexList(indexList);
            long e1 = System.currentTimeMillis();
            removeWithOld(indexList,indexpath.replace(".index","_rmDup"));
            long end = System.currentTimeMillis();
            System.out.println(" 原算法:::::: sort =" + (e1 - s1)+"; rmDup="+(end-e1)+"; total="+(end-s1));
//            ReadAndSort.Sortdat(indexList,indexpath.replace(".index",".dat"));
//            long end = System.currentTimeMillis();
//            System.out.println(" 原算法::::::sort index=" + (e1 - s1) + ";  sort dat="+(end-e1)+";    total="+(end-s1));

//            s1 = System.currentTimeMillis();
//            //removeDuplicate(indexList);
//            ReadAndSort.SortByIndexList(indexList);
//            e1 = System.currentTimeMillis();
//            ReadAndSort.Sortdat(indexList,indexpath.replace(".index",".dat"));
//            end = System.currentTimeMillis();
//            System.out.println("利用List特性::::::sort index=" + (e1 - s1) + "  sort dat="+(end-e1)+"    total="+(end-s1));
//
//            s1 = System.currentTimeMillis();
//            ReadAndSort.SortByIndexList(indexList);
//            e1 = System.currentTimeMillis();
//            //removeWithFor(indexList);
//            ReadAndSort.Sortdat(indexList,indexpath.replace(".index",".dat"));
//            end = System.currentTimeMillis();
//            System.out.println("利用双重for::::::sort index=" + (e1 - s1) + "  sort dat="+(end-e1)+"    total="+(end-s1));
//
//            /*
//            s1 = System.currentTimeMillis();
//            //removeWithBit(indexList);
//            ReadAndSort.SortByIndexList(indexList);
//            e1 = System.currentTimeMillis();
//            ReadAndSort.Sortdat(indexList,indexpath.replace(".index",".dat"));
//            end = System.currentTimeMillis();
//            System.out.println("利用Bitmap排序操作执行时间：" + (end - s) + "ms");
//            System.out.println("利用双重for::::::sort index=" + (e1 - s1) + "  sort dat="+(end-e1)+"    total="+(end-s1));
//*/
//
            s1 = System.currentTimeMillis();
            removeWithHashset(indexList);
            e1 = System.currentTimeMillis();
            ReadAndSort.SortByIndexList(indexList);
            //ReadAndSort.Sortdat(indexList,indexpath.replace(".index",".dat"));
            end = System.currentTimeMillis();
            System.out.println("利用hash特性:::::: rmDup=" + (e1 - s1) + "  sort="+(end-e1)+"    total="+(end-s1));

            s1 = System.currentTimeMillis();
            removeWithRoarBitmapAPI(indexList);
            e1 = System.currentTimeMillis();
            ReadAndSort.SortByIndexList(indexList);
            //ReadAndSort.Sortdat(indexList,indexpath.replace(".index",".dat"));
            end = System.currentTimeMillis();
            System.out.println("利用RoarBitmapAPI::::::rmDup=" + (e1 - s1) + "  sort="+(end-e1)+"    total="+(end-s1));

        }


    }

    public static void removeWithOld(List<PacketIndex> indexList,String outputIndex) {
        BufferedWriter writer=null;
        int c=0;

        try {
            writer=new BufferedWriter(new FileWriter(outputIndex));
            PacketIndex last = indexList.get(0);
            writer.write(last.getTimeSec()+","+last.getPackCount()+","+last.getLocation()+"\n");
            int count = 0;
            for(int i=1;i<indexList.size();i++){
                PacketIndex current = indexList.get(i);
                if((current.timeSec == last.timeSec) &&(current.packCount == last.packCount)){
                    count++;
                    //System.out.println("i="+i+" current"+current.timeSec+","+current.packCount);
                    continue;
                }else {
                    c++;
                    writer.write(current.getTimeSec()+","+current.getPackCount()+","+current.getLocation()+"\n");
                    last = current;
                }

            }
            writer.flush();
            writer.close();
            System.out.println("i="+c);
            System.out.println("重合量="+count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void removeWithBitMap(List<PacketIndex> indexList) {
        /*todo:这里思路有些问题，对于str要先转换成long类型再放入bitmap中，但是在转换这个过程中将str作为key
        *  放入了hashmap，所以在转换这一步就已经利用hashmap的特性进行去重了，并没有使用到bitmap，后续将long类型
        * 存入bitmap，可能只起到了一个减小内存占用的作用，后续也许可以用来进行count distinct*/
        ArrayList<String> list = new ArrayList<>();
        for(PacketIndex index:indexList){
            list.add(index.timeSec+","+index.packCount);
        }
        StrToBitMap strToBitMap = new StrToBitMap();
        strToBitMap.addBitMap(list);
        for (int i=0;i<list.size();i++){
            long index = strToBitMap.getBitMap().get(list.get(i));
        }
    }

    //运行时间：9039106ms，太慢了
    /*
    * 思路有问题：：：创建的bits数组是根据所能保存的最大数据的大小来确定的，如果最大数是100，那么只要new byte[100/8+1],
    而本题中，由于是string经过hash后得到的int值，也就是说可能保存的最大值是int类型所能表示的最大值2^32-1，
    * 因此一开始设置的由索引行数推算出来的数组大小并不合适
     */
    private static void removeWithBit(List<PacketIndex> indexList){
        //byte[] bits = new byte[600000000];
        BitMap bitmap = new LongMap();
        for(int i=0;i<indexList.size();i++){
            PacketIndex index =indexList.get(i);
            //String str = index.timeSec+","+index.packCount;
            long indexLong = index.getTimeSec()*index.getTimeSec();
            System.out.println(indexLong);
            //int hash = bitmap.stringHash(str);
            if(bitmap.add(indexLong)){
                continue;
            }else {
                indexList.remove(i);
                i--;
            }
        }
    }

    //运行时间46455ms
    public static void removeDuplicate(List<PacketIndex> list) {
        List<PacketIndex> result = new ArrayList<PacketIndex>(list.size());
        for (PacketIndex index : list) {
            if (!result.contains(index)) {
                result.add(index);
            }
        }
        list.clear();
        list.addAll(result);
    }

    //运行时间6666ms
    public static void removeWithFor(List<PacketIndex> list){
        for (int i = 0; i < list.size(); i++) {
            PacketIndex pi= list.get(i);
            for (int j = i + 1; j < list.size(); j++) {
                PacketIndex pj= list.get(j);
                if (pi.timeSec == pj.timeSec) {
                    if(pi.packCount == pj.packCount){
                        list.remove(j);
                        j--;
                    }
                }
            }
        }
    }

    public static void removeWithHashset(List<PacketIndex> list) {
        HashSet<String> set = new HashSet<String>(list.size());
        List<PacketIndex> result = new ArrayList<PacketIndex>(list.size());
        for (PacketIndex index : list) {
            String str = index.timeSec+","+index.packCount;
            if (set.add(str)) {
                result.add(index);
            }
        }
        list.clear();
        list.addAll(result);
    }

}
