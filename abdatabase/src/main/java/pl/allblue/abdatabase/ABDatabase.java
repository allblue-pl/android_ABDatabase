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

    static private ReentrantLock lock = null;
    static private DatabaseHelper dbHelper = null;
    static private SQLiteDatabase db = null;

    static private Handler requestHandler = null;
    static private HandlerThread requestHandler_Thread = null;

    static private int transaction_NextId = 0;
    static private Integer transaction_CurrentId = null;


    static public void deleteDatabase(Context context) {
        context.deleteDatabase("ab-database");
    }

    static public boolean isDebug() {
        return false;
    }


    public ABDatabase(final Context context) {
        if (ABDatabase.lock == null)
            ABDatabase.lock = new ReentrantLock();
        ABDatabase.lock.lock();

        if (ABDatabase.requestHandler == null) {
            ABDatabase.requestHandler_Thread = new HandlerThread("ABDatabase");
            ABDatabase.requestHandler_Thread.start();
            ABDatabase.requestHandler = new Handler(
                    ABDatabase.requestHandler_Thread.getLooper());
        }

        if (ABDatabase.dbHelper == null)
            ABDatabase.dbHelper = new DatabaseHelper(context);

        if (ABDatabase.db == null) {
            ABDatabase.db = ABDatabase.dbHelper.getWritableDatabase();
            ABDatabase.db.enableWriteAheadLogging();

            ABDatabase.transaction_CurrentId = null;
        }

        ABDatabase.lock.unlock();
    }

    public void close(final Runnable callback) {
        ABDatabase.requestHandler.post(() -> {
            ABDatabase.lock.lock();

            if (ABDatabase.db != null)
                ABDatabase.db.close();
            ABDatabase.db = null;
            if (callback != null)
                ABDatabase.requestHandler.post(callback);

            ABDatabase.lock.unlock();
        });
    }

    public void getTableColumnInfos(String tableName, Integer transactionId,
            Result.OnTableColumnInfos resultCallback) {
        requestHandler.post(() -> {
            lock.lock();

            String transactionError = validateTransactionId(transactionId);
            if (transactionError != null) {
                resultCallback.onError(new ABDatabaseException(
                        "Cannot get table column infos -> " + transactionError));
                lock.unlock();
                return;
            }

            Cursor c = db.rawQuery("PRAGMA TABLE_INFO('" +
                    tableName + "')", null);
            ColumnInfo[] columnInfos = new ColumnInfo[c.getCount()];

            int i = 0;
            while (c.moveToNext()) {
                ColumnInfo columnInfo = new ColumnInfo(c.getString(1),
                        c.getString(2), c.getInt(3) != 0);
                columnInfos[i] = columnInfo;
                i++;
            }
            c.close();

            lock.unlock();

            resultCallback.onResult(columnInfos);
        });
    }

    public void getTableColumnInfos(String tableName,
            Result.OnTableColumnInfos resultCallback) {
        getTableColumnInfos(tableName, null, resultCallback);
    }

    public void getTableNames(Integer transactionId,
            Result.OnTableNames resultCallback) {
        requestHandler.post(() -> {
            lock.lock();

            String transactionError = validateTransactionId(transactionId);
            if (transactionError != null) {
                resultCallback.onError(new ABDatabaseException(
                        "Cannot get table column infos. " + transactionError));
                lock.unlock();
                return;
            }

            Cursor c = db.rawQuery(
                    "SELECT name FROM sqlite_master WHERE" +
                    " type ='table'" +
                    " AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'android_%'", null);
            String[] tableNames = new String[c.getCount()];

            int i = 0;
            while (c.moveToNext()) {
                tableNames[i] = c.getString(0);
                i++;
            }
            c.close();

            lock.unlock();

            resultCallback.onResult(tableNames);
        });
    }

    public void getTableNames(Result.OnTableNames resultCallback) {
        getTableNames(null, resultCallback);
    }

    public void transaction_Finish(int transactionId, boolean commit,
            Transaction.OnFinish resultCallback) {
        if (isDebug())
            Log.d("ABDatabase", "Transaction - Finish", new Exception());

        requestHandler.post(() -> {
            lock.lock();

            if (transaction_CurrentId == null) {
                lock.unlock();
                resultCallback.onError(new ABDatabaseException(
                        "No transaction in progress."));
                return;
            }

            if (!transaction_CurrentId.equals(transactionId)) {
                lock.unlock();
                resultCallback.onError(new ABDatabaseException(
                        "Wrong transaction id: " + transactionId +
                        ". Current transaction id: " +
                        transaction_CurrentId + "."));
                return;
            }

            if (commit)
                db.setTransactionSuccessful();
            db.endTransaction();

            transaction_CurrentId = null;

            lock.unlock();

            resultCallback.onResult();
        });
    }

    public void transaction_IsAutocommit(
            Transaction.OnIsAutocommit resultCallback) {
        if (isDebug()) {
            Log.d("ABDatabase", "Transaction - Is Autocommit",
                    new Exception());
        }

        requestHandler.post(() -> {
            lock.lock();

            boolean inTransaction = db.inTransaction();

            if (inTransaction != (transaction_CurrentId != null)) {
                lock.unlock();
                resultCallback.onError(new ABDatabaseException(
                        "Transaction id inconsistency."));
                return;
            }

            lock.unlock();

            resultCallback.onResult(transaction_CurrentId);
        });
    }

    public void transaction_Start(Transaction.OnStart resultCallback,
            int timeout) {
        if (isDebug())
            Log.d("ABDatabase", "Transaction - Start", new Exception());

        requestHandler.post(() -> {
            lock.lock();

            if (transaction_CurrentId != null) {
                lock.unlock();
                if (timeout <= 0) {
                    resultCallback.onError(new ABDatabaseException(
                            "Other transaction already in progress: " +
                            transaction_CurrentId));
                } else {
                    requestHandler.postDelayed(() -> {
                        this.transaction_Start(resultCallback,
                                timeout - 500);
                    }, 500);
                }
                return;
            }

            db.beginTransactionNonExclusive();

            transaction_CurrentId = transaction_NextId;
            transaction_NextId++;

            lock.unlock();

            resultCallback.onResult(transaction_CurrentId);
        });
    }

    public void transaction_Start(Transaction.OnStart resultCallback) {
        this.transaction_Start(resultCallback, 0);
    }

    public void query_Execute(String query, Integer transactionId,
            Result.OnResult_ThrowsException resultCallback, int timeout) {
        if (isDebug())
            Log.d("ABDatabase", "Execute: " + query, new Exception());

        requestHandler.post(() -> {
            lock.lock();

            /* Transaction Check */
            if (transactionId == null) {
                if (transaction_CurrentId != null) {
                    lock.unlock();
                    if (timeout <= 0) {
                        resultCallback.onError(new ABDatabaseException(
                                "Transaction in progress: " +
                                        transaction_CurrentId +
                                        ". Cannot run query without transaction: " + query));
                    } else {
                        requestHandler.postDelayed(() -> {
                            this.query_Execute(query, transactionId,
                                    resultCallback, timeout - 500);
                        }, 500);
                    }
                    return;
                }
            } else {
                if (transaction_CurrentId == null) {
                    lock.unlock();
                    resultCallback.onError(new ABDatabaseException(
                            "Wrong transaction id: " + transactionId +
                                    ". No current transaction." +
                                    " Cannot run query: " + query));
                    return;
                }

                if (!transaction_CurrentId.equals(transactionId)) {
                    lock.unlock();
                    resultCallback.onError(new ABDatabaseException(
                            "Wrong transaction id: " + transactionId +
                            ". Current transaction id: " +
                            transaction_CurrentId +
                            ". Cannot run query: " + query));
                    return;
                }
            }
            /* / Transaction Check */

            try {
                db.execSQL(query);
            } catch (SQLiteException e) {
                lock.unlock();
                resultCallback.onError(e);
                return;
            }

            lock.unlock();

            resultCallback.onResult();
        });
    }

    public void query_Execute(String query, Integer transactionId,
            Result.OnResult_ThrowsException resultCallback) {
        this.query_Execute(query, transactionId, resultCallback, 0);
    }

    public void query_Select(String query, SelectColumnType[] columnTypes,
            Integer transactionId, Result.OnSelect resultCallback,
            int timeout) {
        if (isDebug())
            Log.d("ABDatabase", "Select: " + query, new Exception());

        requestHandler.post(() -> {
            lock.lock();

            /* Transaction Check */
            if (transactionId == null) {
                if (transaction_CurrentId != null) {
                    lock.unlock();
                    if (timeout <= 0) {
                        resultCallback.onError(new ABDatabaseException(
                                "Transaction in progress: " +
                                transaction_CurrentId +
                                ". Cannot run query without transaction id: " + query));
                    } else {
                        requestHandler.postDelayed(() -> {
                            this.query_Select(query, columnTypes, transactionId,
                                    resultCallback, timeout - 500);
                        }, 500);
                    }
                    return;
                }
            } else {
                if (transaction_CurrentId == null) {
                    lock.unlock();
                    resultCallback.onError(new ABDatabaseException(
                            "Wrong transaction id: " + transactionId +
                                    ". No current transaction." +
                                    " Cannot run query: " + query));
                    return;
                }

                if (!transaction_CurrentId.equals(transactionId)) {
                    lock.unlock();
                    resultCallback.onError(new ABDatabaseException(
                            "Wrong transaction id: " + transactionId +
                            ". Current transaction id: " +
                            transaction_CurrentId +
                            ". Cannot run query: " + query));
                    return;
                }
            }
            /* / Transaction Check */

            Cursor c = null;
            try {
                c = db.rawQuery(query, null);
            } catch (SQLiteException e) {
                lock.unlock();
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
                            row.put(json);
                        } else if (columnTypes[j] == SelectColumnType.Long)
                            row.put(c.getLong(j));
                        else if (columnTypes[j] == SelectColumnType.String)
                            row.put(c.getString(j));
//                        else {
//                            Lock.unlock();
//                            resultCallback.onError(new ABDatabaseException(
//                                    "Unknown column type '" + columnTypes[j] +
//                                            "'."));
//                            return;
//                        }
                    }
                } catch (JSONException e) {
                    lock.unlock();
                    resultCallback.onError(e);
                    return;
                }

                rows.add(row);
            }
            c.close();

            lock.unlock();

            resultCallback.onResult(rows);
        });
    }

    public void query_Select(String query, List<SelectColumnType> columnTypes,
            Integer transactionId, Result.OnSelect resultCallback,
            int timeout) {
        SelectColumnType[] columnTypesArr =
                new SelectColumnType[columnTypes.size()];
        columnTypes.toArray(columnTypesArr);
        this.query_Select(query, columnTypesArr, transactionId, resultCallback,
                timeout);
    }

    public void query_Select(String query, SelectColumnType[] columnTypes,
            Integer transactionId, Result.OnSelect resultCallback) {
        this.query_Select(query, columnTypes, transactionId, resultCallback,
                0);
    }

    public void query_Select(String query, List<SelectColumnType> columnTypes,
            Integer transactionId, Result.OnSelect resultCallback) {
        this.query_Select(query, columnTypes, transactionId, resultCallback,
                0);
    }


    private String validateTransactionId(Integer transactionId) {
        if (transaction_CurrentId == null) {
            if (transactionId == null)
                return null;

            return "Wrong transaction id. No current transaction.";
        }

        if (transactionId == null) {
            return "Cannot run without transaction. Current transaction id: " +
                    transaction_CurrentId;
        }

        if (transaction_CurrentId != transactionId) {
            return "Cannot run with transaction id '" + transactionId +
                    "'. Current transaction id: " + transaction_CurrentId;
        }

        return null;
    }

}
