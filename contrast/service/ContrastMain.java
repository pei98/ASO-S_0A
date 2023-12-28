package com.test.contrast.service;

import com.sun.org.apache.bcel.internal.generic.ARETURN;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import com.test.contrast.model.PacketIndex;
import org.apache.directory.shared.kerberos.codec.apRep.actions.ApRepInit;
import org.codehaus.jackson.JsonToken;
import scala.Int;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.math.BigInteger;
import java.util.*;

public class ContrastMain {
    public static byte[] byteArr_E225 = hexToByteArr("E225");
    public static int packetLength = 4032;
    public static final char HexCharArr[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    public static final String HexStr = "0123456789ABCDEF";
    public static HashSet<String> sci_channel_set = new HashSet<>(Arrays.asList("0109", "011A", "013F", "014F", "015C", "040B"));


    public static void main(String[] args) throws IOException {
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        String input = "G:\\ASO-S\\data\\1024\\input\\ks\\KX07_S1_000219_20221024_KS5K1_R0_110010.dat";
        //String input = "F:\\ASO-S\\data\\0627\\server03\\KX07_S1_062719_20220627_KS1F1_R0_100101.dat";
        //String input = "F:\\ASO-S\\data\\0627\\server03\\KX07_S1_062719_20220627_KS1F1_R0_110010.dat";
        //String input="E:\\data\\ASO-S\\guidang\\15\\110010\\combine.dat";
        long s1 = System.currentTimeMillis();
        //String domainData = getDataFromFrameDatFile(input);
        //String domainData = "E:\\data\\ASO-S\\0627\\19\\KX07_S1_062719_20220627_KS1F1_R0_100101_DataDomain.dat";
        //String domainData = "E:\\data\\ASO-S\\guidang\\16\\110010\\16_110010_DataDomain.dat";
        String domainData = "G:\\ASO-S\\data\\1024\\input\\ks\\KX07_S1_000219_20221024_KS5K1_R0_110010_DataDomain.dat";
        long s2 = System.currentTimeMillis();
        System.out.println("拼接DomainData数据，执行时间：" + (s2 - s1) + " ms");

        HashMap<String, ArrayList<PacketIndex>> indexmap = readVcidDatFile(domainData);
        System.out.println(indexmap.size());
//        int c= 0;
//        for (String apid:indexmap.keySet()){
//            if (indexmap.get(apid).size()>20){
//                System.out.println(apid+" "+indexmap.get(apid).size());
//                c++;
//            }
//        }
//        System.out.println(c);

//        String index_020A=domainData.replace("DataDomain.dat","020A_index");
//        BufferedWriter writer = new BufferedWriter(new FileWriter(index_020A));
//        ArrayList<PacketIndex> list_020A = indexmap.get("020A");
//        for (PacketIndex index:list_020A){
//            String indexLine = index.getIndexLine();
//            writer.write(indexLine);
//        }
//        writer.flush();
//        writer.close();

        long s3 = System.currentTimeMillis();
//        ArrayList<PacketIndex> indexlist = indexmap.get("020A");
//        ReadAndSort.SortByIndexList(indexlist);
        System.out.println("获取index信息，执行时间：" + (s3 - s2) + " ms");
        for (String apid : indexmap.keySet()) {
            ArrayList<PacketIndex> list = indexmap.get(apid);
//            if (list.size() > 20) {
                long t2 = System.currentTimeMillis();
                //System.out.println("       读取"+apid+"文件，执行时间："+(t2-t1)+" ms");
                ReadAndSort.SortByIndexList(list);
                long t3 = System.currentTimeMillis();
                System.out.println("       index排序，执行时间：" + (t3 - t2) + " ms");
                //logger.info("源包"+apid+" 排序完成");
                ReadAndSort.Sortdat(list, domainData, apid);
                long t4 = System.currentTimeMillis();
                System.out.println("       dat排序，执行时间：" + (t4 - t3) + " ms");
                System.out.println("       排序" + apid + "（排序索引、dat，共执行时间：）" + (t4 - t2));
                System.out.println();
//            }
        }

//        for(String apid:index_filepath_map.keySet()){
//            String indexpath=index_filepath_map.get(apid);
//            //String datpath=indexpath.replace(".index",".dat");
//            long t1 = System.currentTimeMillis();
//            List<PacketIndex> indexList = ReadAndSort.getIndexList(indexpath);
//            if (indexList.size()<=20){
//                continue;
//            }
//            System.out.println("indexlist.size="+indexList.size());
//            for (int i=0;i<indexList.size();i++){
//                PacketIndex index = indexList.get(i);
//                //System.out.print(index.getIndexLine());
//            }
//            long t2 = System.currentTimeMillis();
//            System.out.println("       读取"+apid+"文件，执行时间："+(t2-t1)+" ms");
//            ReadAndSort.SortByIndexList(indexList);
//            long t3 = System.currentTimeMillis();
//            System.out.println("       index排序，执行时间："+(t3-t2)+" ms");
//            //logger.info("源包"+apid+" 排序完成");
//            ReadAndSort.Sortdat(indexList,domainData,apid);
//            long t4 = System.currentTimeMillis();
//            System.out.println("       dat排序，执行时间："+(t4-t3)+" ms");
//            System.out.println("       排序"+apid+"（排序索引、dat，共执行时间：）"+(t4-t1));
//            System.out.println();
//         }

    long s4 = System.currentTimeMillis();
        System.out.println("获取index信息，执行时间："+(s4-s1)+" ms");
    }

    private static HashMap<String, ArrayList<PacketIndex>> readVcidDatFile(String domainData) {
    //private static HashMap<String, String> readVcidDatFile(String domainData) {
        HashMap<String,String> result_indexfile_map=new HashMap<>();
        //String indexOutPath=fileObj.result_outpath;
        BufferedInputStream inputStream=null;
        try {
            inputStream=new BufferedInputStream(new FileInputStream(domainData));
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }

        //缓存快设置为100帧
        byte[] bytes=new byte[884*100];
        //int blockLength=0;
        long startLoc=0;
        byte[] lastData_buffer=null;
        HashMap<String, ArrayList<PacketIndex>> indexmap=new HashMap<>();//用于存储源包索引信息
        //HashMap<String,BufferedWriter> indexWriterMap=new HashMap<>();
        long totalline=0;
        while(true){
            try {
                int blockLength=inputStream.read(bytes);
                //System.out.print(blockLength);
                if(blockLength==-1){
                    break;
                }else{
                    byte[] block = bytes;
                    if(lastData_buffer!=null){
                        byte[] tempBytes=new byte[lastData_buffer.length+blockLength];
                        System.arraycopy(lastData_buffer,0,tempBytes,0,lastData_buffer.length);
                        System.arraycopy(bytes,0,tempBytes,lastData_buffer.length,blockLength);
                        startLoc=startLoc-lastData_buffer.length;
                        block=tempBytes;
                        blockLength=lastData_buffer.length+blockLength;
                    }
                    lastData_buffer=getPacketIndexFromByteArr(startLoc,blockLength,block,indexmap);
                    startLoc+=blockLength;
                    if(totalline%1000==0){
                        System.out.println(totalline+"*100行数据");
                        //logger.info(totalline+"*1000行数据");
                        //每存储1000行索引信息输出一遍map
//                BufferedWriter writer=null;
//                for (String apid:indexmap.keySet()){
//                    if(indexWriterMap.containsKey(apid)){
//                        writer=indexWriterMap.get(apid);
//                    }else{
//                        try {
////                            String index_file_path = domainData.replace("DataDomain.dat",apid+".index");
////                            if(!result_indexfile_map.containsKey(apid)){
////                                result_indexfile_map.put(apid,index_file_path);
////                            }
////                            writer=new BufferedWriter(
////                                    new FileWriter(index_file_path));
//
//                            indexWriterMap.put(apid,writer);
//                            //todo:为了便于调试，这里将IOException改为Exception
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    ArrayList<PacketIndex> list=indexmap.get(apid);
//                    try {
//                        for(PacketIndex packetIndex:list ){
//                            //System.out.print(packetIndex.getIndexLine());
//                            writer.write(packetIndex.getIndexLine());
//                        }
//                        writer.flush();
//                        list.clear();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
                    }
                    totalline++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        System.out.println("totalline="+totalline);
        //将indexmap中剩余部分输出
//        BufferedWriter writer=null;
//        for (String apid:indexmap.keySet()){
//            if(indexWriterMap.containsKey(apid)){
//                writer=indexWriterMap.get(apid);
//            }else{
//                try {
//                    String index_file_path = domainData.replace("DataDomain.dat",apid+".index");
//                    if(!result_indexfile_map.containsKey(apid)){
//                        result_indexfile_map.put(apid,index_file_path);
//                    }
//                    writer=new BufferedWriter(
//                            new FileWriter(index_file_path));
//                    indexWriterMap.put(apid,writer);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            ArrayList<PacketIndex> list=indexmap.get(apid);
//            try {
//                for(PacketIndex packetIndex:list ){
//                    writer.write(packetIndex.getIndexLine());
//                }
//                writer.flush();
//                list.clear();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        //关闭输入输出流
        try {
            inputStream.close();
//            for(BufferedWriter writer1:indexWriterMap.values()){
//                writer1.flush();
//                writer1.close();
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return  indexmap;
        //return  result_indexfile_map;
    }

    private static byte[] getPacketIndexFromByteArr(long startLoc, int blockLength, byte[] block, HashMap<String, ArrayList<PacketIndex>> indexmap) {
        //需要从block中寻找初所有的e225位置
        List<Integer> e225_loc_list=findByteArrFromByteArr(block,0,blockLength,byteArr_E225);
        if (e225_loc_list==null){
            return null;
        }else if(e225_loc_list.size()==0){
            return null;
        }else{
            int end_loc=0;
            for(int i=0;i<e225_loc_list.size();i++){
                int e225_loc=e225_loc_list.get(i);
                if(blockLength-e225_loc<packetLength){
                    //剩余的数据不够一个源包的长度
                    byte[] lastbytes=new byte[blockLength-e225_loc];
                    System.arraycopy(block,e225_loc,lastbytes,0,blockLength-e225_loc);
                    return lastbytes;
                }else{
                    String apid=getApidFromPacket(e225_loc,block);
                    //System.out.println(apid);
                    if (sci_channel_set.contains(apid)){
                        if (judgePacket(startLoc,e225_loc,block)==1){
                            ArrayList<PacketIndex> indexList=null;
                            if(indexmap.containsKey(apid)){
                                indexList=indexmap.get(apid);
                            }else{
                                indexList=new ArrayList<>();
                            }
                            long time = getTimeSecFromPacket(e225_loc, block);
                            int count = getPacketCount(e225_loc, block);
                            long loc = e225_loc+ startLoc;
                            PacketIndex packetIndex=new PacketIndex();
                            packetIndex.timeSec=time;
                            packetIndex.packCount=count;
                            packetIndex.location=loc;
                            //System.out.println(packetIndex.getIndexLine());
                            indexList.add(packetIndex);
                            indexmap.put(apid,indexList);
                            end_loc=e225_loc+packetLength;
                        }else{
                            end_loc=e225_loc;
                        }
                    }

                }
            }
            if(blockLength-end_loc>=packetLength){
                //如果从最后一个e225到结尾的长度大于包长，
                //判断最后一个字节是否为E2
                if(block[blockLength-1]==byteArr_E225[0]){
                    byte[] arr=new byte[1];
                    arr[0]=byteArr_E225[0];
                    return arr;
                }else{
                    return null;
                }
            }
            return null;
        }
    }

    private static int judgePacket(long blocverskloc,int e225_loc,byte[] bytes) {
        int result=1;

        if (bytes.length - 128 < 8 + e225_loc) { //去除验证域
            result =0;
        }
        //判断版本号
        String version_hd_Bin = resolveDataByLocation(e225_loc * 8 + 16, 3, bytes);
        //if (!version_hd_Bin.equals("000")) result=0;
        if (!("000".equals(version_hd_Bin))) {
            result = 0;
        }


        //判断包类型
        String packet_type = resolveDataByLocation(e225_loc * 8 + 19, 1, bytes);
        if(!(Integer.parseInt(packet_type)==0)) {
            result =0;
        }

        //判断副导头
        String fudaotou = resolveDataByLocation(e225_loc * 8 + 20, 1, bytes);
        if(!(Integer.parseInt(fudaotou)==1)) {
            result =0;
        }

        //判断时间码前两字节是否为0；
        byte[] fudaotou_xinxi = getBytesByByteL(e225_loc + 8, 2, bytes);
        if(!(fudaotou_xinxi[0]==0 && fudaotou_xinxi[1]==0)) {
            result =0;
        }

        //判断包长
        int packetLength = getPacketLength(bytes, e225_loc);
        if (bytes.length - 128 - e225_loc < packetLength) {
            result=2;
        }
        if (bytes.length - 128 - e225_loc >= packetLength){
            result=1;
        }
        return result;
    }

    private static int getPacketLength(byte[] packet, int location) {
        if (packet.length < 8) {
            System.out.println("包长度小于8字节，无法获取包长数据，请检查数据");
            return -1;
        }
        byte[] length_byteArr =new byte[2];
        length_byteArr[0]=packet[location+6];
        length_byteArr[1]=packet[location+7];
        String length_hexStr =binary(length_byteArr,16);
        int length = Integer.parseInt(length_hexStr, 16) + 1 + 8;
        //注意，这个包长不包括同步码
        return length;
    }

    private static String resolveDataByLocation(int startLocation,int bitLen,byte[] bytes) {
        StringBuilder stringBuilder=new StringBuilder();
        int startByte=startLocation/8;
        int startBit=startLocation%8;
        int endByte=(startLocation+bitLen-1)/8;
        int endBit=(startLocation+bitLen-1)%8;
        //首先将startByte和endByte之间的所有字节转换为二进制字符串
        for(int i=startByte;i<=endByte;i++){
            stringBuilder.append(byteToBinaryStr(bytes[i]));
        }
        int start=startBit;
        int end=(endByte-startByte)*8+endBit+1;
        String value=stringBuilder.toString().substring(start,end);

        return value;
    }

    private static long getTimeSecFromPacket(int e225_loc, byte[] bytes) {
        byte[] bytes1=getBytesByByteL(e225_loc+10,4,bytes);
        long timesec=Long.parseLong(binary(bytes1,10));
        return timesec;
    }

    private static String binary(byte[] bytes, int radix) {
        return new BigInteger(1, bytes).toString(radix);
    }

    private static byte[] getBytesByByteL(int startL,int byteLen,byte[] bytes) {
        int start=startL;
        int end=(startL+byteLen);
        byte[] result=new byte[byteLen];

        for(int i=start,j=0;i<end;i++,j++){
            byte a=bytes[i];
            result[j]=a;
        }
        return result;
    }

    private static int getPacketCount(int e225_loc, byte[] bytes) {
        byte b0=bytes[e225_loc+4];
        byte b1=bytes[e225_loc+5];
        String bin=byteToBinaryStr(b0).substring(2)+byteToBinaryStr(b1);
        return Integer.parseInt(bin,2);
    }

    public static String getApidFromPacket(int e225_loc,byte[] bytes){
        String str=resolveDataByLocation(e225_loc*8+21,11,bytes);
//        byte b0=bytes[e225_loc+2];
//        byte b1=bytes[e225_loc+3];
//        String result_binary=byteToBinaryStr(b0).substring(5,8)+byteToBinaryStr(b1);
//        String result= Integer.toHexString(Integer.parseInt(result_binary,2)).toUpperCase();
        String result = Integer.toHexString(Integer.parseInt(str, 2)).toUpperCase();
        while (result.length()<4){
            result="0"+result;
        }
        return  result;
    }

    public  static String byteToBinaryStr(byte b){
        String result=Integer.toBinaryString((b & 0xFF)+0x100).substring(1);
        return result;
    }

    private static List<Integer> findByteArrFromByteArr(byte[] longArr,int startl,int length, byte[] shortArr) {
        //从长数组中找出短数组的位置,如果没有找到list的长度为0，
        ArrayList<Integer> result=new ArrayList<>();
        int sl=shortArr.length;//短字节数组长度
        int j=0;
        for (int i=startl;i<length-sl+1;i++){
            if(longArr[i]==shortArr[0]){

                for(j=1;j<sl;j++){
                    if(longArr[i+j]!=shortArr[j]){
                        break;
                    }
                }
                if(j==sl){
                    result.add(i);
                }
            }
        }
        return  result;
    }

    private static String getDataFromFrameDatFile(String input) {
        String data_domain_file=input.replace(".dat","_DataDomain.dat");
        BufferedInputStream inputStream=null;
        BufferedOutputStream outputStream=null;
        System.out.println("拼接帧数据文件的数据域");
        //logger.info("拼接帧数据文件的数据域");
        try {
            inputStream=new BufferedInputStream(new FileInputStream(input));
            outputStream=new BufferedOutputStream(new FileOutputStream(data_domain_file));
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }
        byte[] reader_byte=new byte[1024];

        long line=0;
        while(true){
            try {
                if(inputStream.read(reader_byte)==-1){
                    break;
                }
                outputStream.write(reader_byte,12,884);

                line++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream.flush();
            outputStream.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data_domain_file;
    }

    public static byte[] hexToByteArr(String hexStr) {
        char[] charArr = hexStr.toCharArray();
        byte btArr[] = new byte[charArr.length / 2];
        int index = 0;
        for (int i = 0; i < charArr.length; i++) {
            int highBit = HexStr.indexOf(charArr[i]);
            int lowBit = HexStr.indexOf(charArr[++i]);
            btArr[index] = (byte) (highBit << 4 | lowBit);
            index++;
        }
        return btArr;
    }

}
