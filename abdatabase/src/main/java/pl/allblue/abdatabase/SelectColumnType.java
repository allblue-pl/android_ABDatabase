package pl.allblue.abdatabase;

public enum SelectColumnType {
    Bool,
    Float,
    Int,
    Long,
    JSON,
    String;

    public static SelectColumnType fromIndex(int index) {
        if (index == 0)
            return Bool;
        if (index == 1)
            return Float;
        if (index == 2)
            return Int;
        if (index == 3)
            return Long;
        if (index == 4)
            return JSON;
        if (index == 5)
            return String;

        throw new AssertionError(
                "Unknown select column type index: " + index);
    }
}
