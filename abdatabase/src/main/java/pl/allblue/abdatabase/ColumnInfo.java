package pl.allblue.abdatabase;

public class ColumnInfo {

    private String name = null;
    private String type = null;
    private boolean notNull = false;

    public ColumnInfo(String name, String type, boolean notNull)
    {
        this.name = name;
        this.type = type;
        this.notNull = notNull;
    }

    String getName()
    {
        return this.name;
    }

    String getType()
    {
        return this.type;
    }

    boolean isNotNull()
    {
        return this.notNull;
    }

}
