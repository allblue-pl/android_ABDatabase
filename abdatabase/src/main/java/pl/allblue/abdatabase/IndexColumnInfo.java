package pl.allblue.abdatabase;

public class IndexColumnInfo {
    private int seq;
    private String name;
    private boolean desc;

    public IndexColumnInfo(int seq, String name, boolean desc) {
        this.seq = seq;
        this.name = name;
        this.desc = desc;
    }

    public String getName() {
        return this.name;
    }

    public int getSeq() {
        return this.seq;
    }

    public boolean isDesc() {
        return this.desc;
    }
}
