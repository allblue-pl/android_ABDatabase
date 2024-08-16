package pl.allblue.abdatabase;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ABDatabase
{

    static private ReentrantLock Lock = null;
    static private DatabaseHelper DBHelper = null;
    static private SQLiteDatabase DB = null;

    static private Handler RequestHandler = null;
    static private HandlerThread RequestHandler_Thread = null;

    static private int Transaction_NextId = 0;
    static private Integer Transaction_CurrentId = null;


    static public void deleteDatabase(Context context)
    {
        context.deleteDatabase("ab-database");
    }

    static public boolean isDebug()
    {
        return true;
    }


    public ABDatabase(final Context context)
    {
        if (ABDatabase.Lock == null)
            ABDatabase.Lock = new ReentrantLock();
        ABDatabase.Lock.lock();

        if (ABDatabase.RequestHandler == null) {
            ABDatabase.RequestHandler_Thread = new HandlerThread("ABDatabase");
            ABDatabase.RequestHandler_Thread.start();
            ABDatabase.RequestHandler = new Handler(
                    ABDatabase.RequestHandler_Thread.getLooper());
        }

        if (ABDatabase.DBHelper == null)
            ABDatabase.DBHelper = new DatabaseHelper(context);

        if (ABDatabase.DB == null) {
            ABDatabase.DB = ABDatabase.DBHelper.getWritableDatabase();
            ABDatabase.DB.enableWriteAheadLogging();

            ABDatabase.Transaction_CurrentId = null;
        }

        ABDatabase.Lock.unlock();
    }

    public void close(final Runnable callback)
    {
        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            if (ABDatabase.DB != null)
                ABDatabase.DB.close();
            ABDatabase.DB = null;
            if (callback != null)
                ABDatabase.RequestHandler.post(callback);

            ABDatabase.Lock.unlock();
        });
    }

    public void getTableColumnInfos(String tableName,
            Result.OnTableColumnInfos resultCallback)
    {
        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            Cursor c = ABDatabase.DB.rawQuery("PRAGMA TABLE_INFO('" +
                    tableName + "')", null);
            ColumnInfo[] columnInfos = new ColumnInfo[c.getCount()];

            int i = 0;
            while (c.moveToNext()) {
                ColumnInfo columnInfo = new ColumnInfo(c.getString(1),
                        c.getString(2), c.getInt(3) != 0);
                columnInfos[i] = columnInfo;
                i++;
            }

            ABDatabase.Lock.unlock();

            resultCallback.onResult(columnInfos);
        });
    }

    public void getTableNames(Result.OnTableNames resultCallback)
    {
        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            Cursor c = ABDatabase.DB.rawQuery(
                    "SELECT name FROM sqlite_master WHERE" +
                    " type ='table'" +
                    " AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'android_%'", null);
            String[] tableNames = new String[c.getCount()];

            int i = 0;
            while (c.moveToNext()) {
                tableNames[i] = c.getString(0);
                i++;
            }

            ABDatabase.Lock.unlock();

            resultCallback.onResult(tableNames);
        });
    }

    public void transaction_Finish(int transactionId, boolean commit,
            Transaction.OnFinish resultCallback)
    {
        if (ABDatabase.isDebug())
            Log.d("ABDatabase", "Transaction - Finish", new Exception());

        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            if (ABDatabase.Transaction_CurrentId == null) {
                ABDatabase.Lock.unlock();
                resultCallback.onError(new ABDatabaseException(
                        "No transaction in progress."));
                return;
            }

            if (!ABDatabase.Transaction_CurrentId.equals(transactionId)) {
                ABDatabase.Lock.unlock();
                resultCallback.onError(new ABDatabaseException(
                        "Wrong transaction id: " + transactionId +
                        ". Current transaction id: " +
                        ABDatabase.Transaction_CurrentId + "."));
                return;
            }

            if (commit)
                ABDatabase.DB.setTransactionSuccessful();
            ABDatabase.DB.endTransaction();

            ABDatabase.Transaction_CurrentId = null;

            ABDatabase.Lock.unlock();

            resultCallback.onResult();
        });
    }

    public void transaction_IsAutocommit(
            Transaction.OnIsAutocommit resultCallback)
    {
        if (ABDatabase.isDebug()) {
            Log.d("ABDatabase", "Transaction - Is Autocommit",
                    new Exception());
        }

        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            boolean inTransaction = ABDatabase.DB.inTransaction();

            if (inTransaction != (ABDatabase.Transaction_CurrentId != null)) {
                ABDatabase.Lock.unlock();
                resultCallback.onError(new ABDatabaseException(
                        "Transaction id inconsistency."));
                return;
            }

            ABDatabase.Lock.unlock();

            resultCallback.onResult(ABDatabase.Transaction_CurrentId);
        });
    }

    public void transaction_Start(Transaction.OnStart resultCallback,
            int timeout)
    {
        if (ABDatabase.isDebug())
            Log.d("ABDatabase", "Transaction - Start", new Exception());

        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            if (ABDatabase.Transaction_CurrentId != null) {
                ABDatabase.Lock.unlock();
                if (timeout <= 0) {
                    resultCallback.onError(new ABDatabaseException(
                            "Other transaction already in progress: " +
                            ABDatabase.Transaction_CurrentId));
                } else {
                    ABDatabase.RequestHandler.postDelayed(() -> {
                        this.transaction_Start(resultCallback,
                                timeout - 500);
                    }, 500);
                }
                return;
            }

            ABDatabase.DB.beginTransactionNonExclusive();

            ABDatabase.Transaction_CurrentId = ABDatabase.Transaction_NextId;
            ABDatabase.Transaction_NextId++;

            ABDatabase.Lock.unlock();

            resultCallback.onResult(ABDatabase.Transaction_CurrentId);
        });
    }

    public void transaction_Start(Transaction.OnStart resultCallback)
    {
        this.transaction_Start(resultCallback, 0);
    }

    public void query_Execute(String query, Integer transactionId,
            Result.OnResult_ThrowsException resultCallback, int timeout)
    {
        if (ABDatabase.isDebug())
            Log.d("ABDatabase", "Execute: " + query, new Exception());

        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            /* Transaction Check */
            if (transactionId == null) {
                if (ABDatabase.Transaction_CurrentId != null) {
                    ABDatabase.Lock.unlock();
                    if (timeout <= 0) {
                        resultCallback.onError(new ABDatabaseException(
                                "Transaction in progress: " +
                                        ABDatabase.Transaction_CurrentId +
                                        ". Cannot run query without transaction: " + query));
                    } else {
                        ABDatabase.RequestHandler.postDelayed(() -> {
                            this.query_Execute(query, transactionId,
                                    resultCallback, timeout - 500);
                        }, 500);
                    }
                    return;
                }
            } else {
                if (!ABDatabase.Transaction_CurrentId.equals(transactionId)) {
                    ABDatabase.Lock.unlock();
                    resultCallback.onError(new ABDatabaseException(
                            "Wrong transaction id: " + transactionId +
                            ". Current transaction id: " +
                            ABDatabase.Transaction_CurrentId +
                            ". Cannot run query: " + query));
                    return;
                }
            }
            /* / Transaction Check */

            try {
                ABDatabase.DB.execSQL(query);
            } catch (SQLiteException e) {
                ABDatabase.Lock.unlock();
                resultCallback.onError(e);
                return;
            }

            ABDatabase.Lock.unlock();

            resultCallback.onResult();
        });
    }

    public void query_Execute(String query, Integer transactionId,
            Result.OnResult_ThrowsException resultCallback)
    {
        this.query_Execute(query, transactionId, resultCallback, 0);
    }

    public void query_Select(String query, SelectColumnType[] columnTypes,
            Integer transactionId, Result.OnSelect resultCallback,
            int timeout)
    {
        if (ABDatabase.isDebug())
            Log.d("ABDatabase", "Select: " + query, new Exception());

        ABDatabase.RequestHandler.post(() -> {
            ABDatabase.Lock.lock();

            /* Transaction Check */
            if (transactionId == null) {
                if (ABDatabase.Transaction_CurrentId != null) {
                    ABDatabase.Lock.unlock();
                    if (timeout <= 0) {
                        resultCallback.onError(new ABDatabaseException(
                                "Transaction in progress: " +
                                ABDatabase.Transaction_CurrentId +
                                ". Cannot run query without transaction id: " + query));
                    } else {
                        ABDatabase.RequestHandler.postDelayed(() -> {
                            this.query_Select(query, columnTypes, transactionId,
                                    resultCallback, timeout - 500);
                        }, 500);
                    }
                    return;
                }
            } else {
                if (!ABDatabase.Transaction_CurrentId.equals(transactionId)) {
                    ABDatabase.Lock.unlock();
                    resultCallback.onError(new ABDatabaseException(
                            "Wrong transaction id: " + transactionId +
                            ". Current transaction id: " +
                            ABDatabase.Transaction_CurrentId +
                            ". Cannot run query: " + query));
                    return;
                }
            }
            /* / Transaction Check */

            Cursor c = null;
            try {
                c = ABDatabase.DB.rawQuery(query, null);
            } catch (SQLiteException e) {
                ABDatabase.Lock.unlock();
                resultCallback.onError(e);
                return;
            }

            List<JSONArray> rows = new ArrayList<>();

            int i = 0;
            while (c.moveToNext()) {
                JSONArray row = new JSONArray();
                try {
                    for (int j = 0; j < columnTypes.length; j++) {
                        if (c.isNull(j))
                            row.put(JSONObject.NULL);
                        else if (columnTypes[j] == SelectColumnType.Bool)
                            row.put(c.getInt(j) == 1);
                        else if (columnTypes[j] == SelectColumnType.Float)
                            row.put(c.getFloat(j));
                        else if (columnTypes[j] == SelectColumnType.Int)
                            row.put(c.getInt(j));
                        else if (columnTypes[j] == SelectColumnType.JSON) {
                            String json_Str = c.getString(j);
                            JSONObject json = new JSONObject(json_Str);
                            row.put(json.get("value"));
                        } else if (columnTypes[j] == SelectColumnType.Long)
                            row.put(c.getLong(j));
                        else if (columnTypes[j] == SelectColumnType.String)
                            row.put(c.getString(j));
                        else {
                            ABDatabase.Lock.unlock();
                            resultCallback.onError(new ABDatabaseException(
                                    "Unknown column type '" + columnTypes[j] +
                                            "'."));
                            return;
                        }
                    }
                } catch (JSONException e) {
                    ABDatabase.Lock.unlock();
                    resultCallback.onError(e);
                    return;
                }

                rows.add(row);
            }

            ABDatabase.Lock.unlock();

            resultCallback.onResult(rows);
        });
    }

    public void query_Select(String query, SelectColumnType[] columnTypes,
            Integer transactionId, Result.OnSelect resultCallback)
    {
        this.query_Select(query, columnTypes, transactionId, resultCallback,
                0);
    }

}
