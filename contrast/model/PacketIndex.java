package com.test.contrast.model;

import java.util.Objects;

public class PacketIndex {
    //源包索引， 第一列 源包秒计数 源包计数  偏移量(long)
    //由于E225源包长度固定，所以不需要在索引中记录源包长度
    public long timeSec;
    public int  packCount;
    public long location;

    public boolean isBpflag() {
        return bpflag;
    }

    public void setBpflag(boolean bpflag) {
        this.bpflag = bpflag;
    }

    public boolean bpflag=false;


    public long getTimeSec() {
        return timeSec;
    }

    public void setTimeSec(long timeSec) {
        this.timeSec = timeSec;
    }

    public int getPackCount() {
        return packCount;
    }

    public void setPackCount(int packCount) {
        this.packCount = packCount;
    }

    public long getLocation() {
        return location;
    }

    public void setLocation(long location) {
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PacketIndex)) return false;
        PacketIndex that = (PacketIndex) o;
        return getTimeSec() == that.getTimeSec() && getPackCount() == that.getPackCount();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTimeSec(), getPackCount(), getLocation());
    }

    public String getIndexLine(){
        String str=timeSec+","+packCount+","+location+"\n";
        return  str;
    }
}
