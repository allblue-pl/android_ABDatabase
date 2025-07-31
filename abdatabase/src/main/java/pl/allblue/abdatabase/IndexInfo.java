package pl.allblue.abdatabase;

public class IndexInfo {

    private String name = null;
    private boolean pk;

    public IndexInfo(String name, boolean pk) {
        this.name = name;
        this.pk = pk;
    }

    public String getName() {
        return this.name;
    }

    public boolean isPK() {
        return this.pk;
    }

}
