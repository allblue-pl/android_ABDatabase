package pl.allblue.abdatabase;

public class ColumnInfo {

    private String name = null;
    private String type = null;
    private boolean notNull = false;
    private boolean isPK = false;

    public ColumnInfo(String name, String type, boolean notNull, boolean isPK) {
        this.name = name;
        this.type = type;
        this.notNull = notNull;
        this.isPK = isPK;
    }

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public boolean isNotNull() {
        return this.notNull;
    }

    public boolean isPK() {
        return this.isPK;
    }

}
