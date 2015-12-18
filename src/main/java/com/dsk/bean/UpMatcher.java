package com.dsk.bean;

/**
 * Created by wanghaixing
 * on 2015/12/17 14:11.
 */
//uid,db,tab,rec,ext,ver,nation,pid,count,date
//XXXXX,stu_t,TAB_STARTUP_APP,aWRtYW4kaWRtYW4uZXhl,RGlzYWJsZQ==,6.7.69,us,isafe,1,20151216
public class UpMatcher {
    private String uid;
    private String db;
    private String tab;
    private String rec;
    private String ext;
    private String ver;
    private String nation;
    private String pid;
    private int count;

    public UpMatcher(String uid, String db, String tab, String rec, String ext, String ver,
                     String nation, String pid, int count) {
        this.uid = uid;
        this.db = db;
        this.tab = tab;
        this.rec = rec;
        this.ext = ext;
        this.ver = ver;
        this.nation = nation;
        this.pid = pid;
        this.count = count;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTab() {
        return tab;
    }

    public void setTab(String tab) {
        this.tab = tab;
    }

    public String getRec() {
        return rec;
    }

    public void setRec(String rec) {
        this.rec = rec;
    }

    public String getExt() {
        return ext;
    }

    public void setExt(String ext) {
        this.ext = ext;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getNation() {
        return nation;
    }

    public void setNation(String nation) {
        this.nation = nation;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

}
