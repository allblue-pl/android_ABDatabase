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

    public String getName()
    {
        return this.name;
    }

    public String getType()
    {
        return this.type;
    }

    public boolean isNotNull()
    {
        return this.notNull;
    }

}
