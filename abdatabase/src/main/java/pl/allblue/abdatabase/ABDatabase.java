package pl.allblue.abdatabase;

import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ABDatabase
{

    static public final String FilePaths_DBRequests = "db-requests.json";
    static public final String FilePaths_DeviceInfo = "device-info.json";
    static public final String FilePaths_TableIds = "table-ids.json";

    static private DatabaseHelper DatabaseHelperInstance = null;


    public SQLiteDatabase db = null;

    public Long deviceInfo_DeviceId = null;
    public String deviceInfo_DeviceHash = null;
    public Long deviceInfo_LastUpdate = null;

    public ABDatabase(final Context context)
    {
        if (ABDatabase.DatabaseHelperInstance == null)
            ABDatabase.DatabaseHelperInstance = new DatabaseHelper(context);
        this.db = ABDatabase.DatabaseHelperInstance.getWritableDatabase();
        final SQLiteDatabase db = this.db;
        final ABDatabase self = this;
    }

    public void close()
    {
        this.db.close();
    }

    ColumnInfo[] getTableColumns(String tableName) throws SQLiteException
    {
        Cursor c = db.rawQuery("PRAGMA TABLE_INFO('" + tableName + "')", null);
        ColumnInfo[] columnInfos = new ColumnInfo[c.getCount()];

        int i = 0;
        while (c.moveToNext()) {
            ColumnInfo columnInfo = new ColumnInfo(c.getString(1), c.getString(2),
                    c.getInt(3) != 0);
            columnInfos[i] = columnInfo;
            i++;
        }

        return columnInfos;
    }

    String[] getTableNames() throws SQLiteException
    {
        Cursor c = db.rawQuery("SELECT name FROM sqlite_master WHERE" +
                " type ='table'" +
                " AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'android_%'", null);
        String[] tableNames = new String[c.getCount()];

        int i = 0;
        while (c.moveToNext()) {
            tableNames[i] = c.getString(0);
            i++;
        }

        return tableNames;
    }

    void transaction_Finish(boolean commit) throws SQLiteException
    {
        if (commit) {
            Log.d("ABDatabase", "Finishing Transaction - Commit");
            db.setTransactionSuccessful();
        } else {
            Log.d("ABDatabase", "Finishing Transaction - Rollback");
        }

        db.endTransaction();
    }

    boolean transaction_IsAutocommit() throws SQLiteException
    {
        return !db.inTransaction();
    }

    void transaction_Start() throws SQLiteException
    {
        db.beginTransaction();
    }

    void query_Execute(String query) throws SQLiteException
    {
        db.execSQL(query);
    }

    JSONArray[] query_Select(String query, String[] columnTypes) throws SQLiteException
    {
        Cursor c = db.rawQuery(query, null);
        JSONArray[] rows = new JSONArray[c.getCount()];

        int i = 0;
        while (c.moveToNext()) {
            JSONArray row = new JSONArray();
            try {
                for (int j = 0; j < columnTypes.length; j++) {
                    if (c.isNull(j))
                        row.put(JSONObject.NULL);
                    else if (columnTypes[j].equals("Bool")) {
                        //                                    Log.d("Test", Integer.toString(c.getInt(i)));
                        row.put(c.getInt(j) == 1);
                    } else if (columnTypes[j].equals("Float"))
                        row.put(c.getFloat(i));
                    else if (columnTypes[j].equals("Id"))
                        row.put(c.getLong(i));
                    else if (columnTypes[j].equals("Int"))
                        row.put(c.getInt(i));
                    else if (columnTypes[j].equals("Long"))
                        row.put(c.getLong(i));
                    else if (columnTypes[j].equals("String"))
                        row.put(c.getString(i));
                    else if (columnTypes[j].equals("Time"))
                        row.put(c.getLong(i));
                    else {
                        throw new SQLiteException("Unknown column type '" +
                                columnTypes[j] + "'.");
                    }
                }
            } catch (JSONException e) {
                throw new SQLiteException("JSONException: " + e.getMessage(), e);
            }

            rows[i] = row;
        }

        return rows;
    }

}
