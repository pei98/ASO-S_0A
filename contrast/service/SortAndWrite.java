package com.test.contrast.service;

import com.test.contrast.model.PacketIndex;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortAndWrite {
    public static void main(String[] args) throws IOException {
        List<String> indexpaths = new ArrayList<String>();
//        indexpaths.add("G:\\ASO-S\\data\\1024\\output\\SCI\\ASO-S_lev0A_wst_T20221023_000218_MY_AA\\ASO-S_lev0A_wst_T20221023_000218_MY_AA_sorted.index");
//        indexpaths.add("G:\\ASO-S\\data\\1024\\output\\SCI\\ASO-S_lev0A_wst_T20221024_000219_KS_AA\\ASO-S_lev0A_wst_T20221024_000219_KS_AA_sorted.index");
//        indexpaths.add("G:\\ASO-S\\data\\1024\\output\\SCI\\ASO-S_lev0A_wst_T20221024_000220_KS_AA\\ASO-S_lev0A_wst_T20221024_000220_KS_AA_sorted.index");
        indexpaths.add("F:\\KX07_S2_000804_20221203_MY4J2_R0_001\\KX07_S2_000804_20221203_MY4J2_R0_110010.index");
        indexpaths.add("F:\\KX07_S2_000805_20221203_MY4P2_R0_001\\KX07_S2_000805_20221203_MY4P2_R0_110010.index");
        BufferedWriter writer=null;

        for (String path:indexpaths){
            ArrayList<PacketIndex> indexList = ReadAndSort.getIndexList(path);
            ReadAndSort.SortByIndexList(indexList);

            String outpath = path.replace(".index","_sorted.index");
            writer=new BufferedWriter(new FileWriter(outpath));
            for (PacketIndex index:indexList){
                writer.write(index.getTimeSec()+","+index.getPackCount()+","+index.getLocation()+"\n");
            }
            writer.flush();
            writer.close();

        }
    }
}
