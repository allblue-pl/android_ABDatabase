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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ABDatabase
{

    static private DatabaseHelper DatabaseHelperInstance = null;


    static public void DeleteDatabase(Context context)
    {
        context.deleteDatabase("ab-database");
    }


    public SQLiteDatabase db = null;


    private Lock lock = null;
    private Integer transaction_NextId = null;
    private Integer transaction_CurrentId = null;


    public ABDatabase(final Context context)
    {
        this.lock = new ReentrantLock();
        this.lock.lock();

        if (ABDatabase.DatabaseHelperInstance == null)
            ABDatabase.DatabaseHelperInstance = new DatabaseHelper(context);
        this.db = ABDatabase.DatabaseHelperInstance.getWritableDatabase();
        this.db.enableWriteAheadLogging();

        this.transaction_NextId = 0;

        this.lock.unlock();
    }

    public void close()
    {
        this.db.close();
    }

    public ColumnInfo[] getTableColumnInfos(String tableName)
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

    public String[] getTableNames()
    {
        Cursor c = this.db.rawQuery("SELECT name FROM sqlite_master WHERE" +
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

    public void transaction_Finish(int transactionId, boolean commit)
            throws ABDatabaseException
    {
        this.lock.lock();

        this.transaction_CurrentId = null;

        this.lock.unlock();
    }

    public boolean transaction_IsAutocommit()
    {
        this.lock.lock();

        boolean inTransaction = this.transaction_CurrentId != null;

        this.lock.unlock();

        return !inTransaction;
    }

    public int transaction_Start() throws ABDatabaseException, SQLException
    {
        this.lock.lock();

        this.transaction_CurrentId = this.transaction_NextId;
        this.transaction_NextId++;

        this.lock.unlock();

        return transaction_CurrentId;
    }

    public void query_Execute(String query, Integer transactionId)
            throws ABDatabaseException
    {
        this.lock.lock();

        try {
            this.db.execSQL(query);
        } catch (SQLiteException e) {
            this.lock.unlock();
            throw new ABDatabaseException(e.getMessage(), e);
        }

        this.lock.unlock();
    }

    public void query_Execute(String query) throws ABDatabaseException
    {
        this.query_Execute(query, null);
    }

    public List<JSONArray> query_Select(String query, String[] columnTypes,
            Integer transactionId) throws ABDatabaseException
    {
        this.lock.lock();

        Cursor c = null;
        try {
            c = db.rawQuery(query, null);
        } catch (SQLiteException e) {
            this.lock.unlock();
            throw new ABDatabaseException(e.getMessage(), e);
        }

        List<JSONArray> rows = new ArrayList<>();

        int i = 0;
        while (c.moveToNext()) {
            JSONArray row = new JSONArray();
            try {
                for (int j = 0; j < columnTypes.length; j++) {
                    if (c.isNull(j))
                        row.put(JSONObject.NULL);
                    else if (columnTypes[j].equals("AutoIncrementId"))
                        row.put(c.getInt(j));
                    else if (columnTypes[j].equals("Bool"))
                        row.put(c.getInt(j) == 1);
                    else if (columnTypes[j].equals("Float"))
                        row.put(c.getFloat(j));
                    else if (columnTypes[j].equals("Id"))
                        row.put(c.getLong(j));
                    else if (columnTypes[j].equals("Int"))
                        row.put(c.getInt(j));
                    else if (columnTypes[j].equals("JSON")) {
                        String json_Str = c.getString(j);
                        JSONObject json = new JSONObject(json_Str);
                        row.put(json.get("value"));
                    } else if (columnTypes[j].equals("Long"))
                        row.put(c.getLong(j));
                    else if (columnTypes[j].equals("String"))
                        row.put(c.getString(j));
                    else if (columnTypes[j].equals("Time"))
                        row.put(c.getLong(j));
                    else {
                        this.lock.unlock();
                        throw new ABDatabaseException("Unknown column type '" +
                                columnTypes[j] + "'.");
                    }
                }
            } catch (JSONException e) {
                this.lock.unlock();
                throw new ABDatabaseException("JSONException: " + e.getMessage(), e);
            }

            rows.add(row);
        }

        this.lock.unlock();

        return rows;
    }

    public List<JSONArray> query_Select(String query, String[] columnTypes)
            throws ABDatabaseException
    {
        return this.query_Select(query, columnTypes, null);
    }


    private void resetConnection()
    {
        this.transaction_CurrentId = null;
    }

}
