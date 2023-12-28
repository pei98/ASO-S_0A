package com.test.contrast.service;

import com.test.contrast.model.PacketIndex;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.List;

public class RoarBitmapAPI {
    public static void main(String[] args) {
        List<String> indexpaths = new ArrayList<String>();
        indexpaths.add("E:\\data\\ASO-S\\output\\0A\\SCI\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA\\ASO-S_lev0A_fmg_T20200611_000102_SY_AA.index");
        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen03.index");
        indexpaths.add("E:\\data\\ASO-S\\index\\GeneratedIndex\\gen04.index");
        indexpaths.add("E:\\data\\ASO-S\\index\\guidang\\16_110010_0909.index");

        for (String indexpath:indexpaths) {
            System.out.println(indexpath);
            List<PacketIndex> indexList = ReadAndSort.getIndexList(indexpath);

            long s = System.currentTimeMillis();
            ReadAndSort.SortByIndexList(indexList);
            removeWithRoarBitmapAPI(indexList);
            long end = System.currentTimeMillis();
            System.out.println("利用Bitmap排序操作执行时间：" + (end - s) + "ms");
        }
    }

    public static void removeWithRoarBitmapAPI(List<PacketIndex> list){
        LongBitmapDataProvider rr = new Roaring64NavigableMap();
        for(int i=0;i<list.size();i++){
            PacketIndex index =list.get(i);
            long t = index.getTimeSec()*index.getPackCount();
            if (rr.contains(t)){
                list.remove(i);
                i--;
            }else {
                rr.addLong(t);
            }
        }
    }
}
