package pl.allblue.abdatabase;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class DatabaseHelper extends SQLiteOpenHelper
{

    static public String EscapeString(String str) {
        if (str == null)
            return "NULL";

        return str
                .replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\0", "\\0")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
//            .replace("\"", "\\\"")
                .replace("\\x1a", "\\Z");
    }


    public DatabaseHelper(Context context) {
        super(context, "ab-database", null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {

    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {

    }

}
