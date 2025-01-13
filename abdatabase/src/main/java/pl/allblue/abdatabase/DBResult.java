package pl.allblue.abdatabase;

import org.json.JSONArray;

import java.util.List;

public class DBResult
{

    static public abstract class OnResult {
        public abstract void onResult();
    }

    static public abstract class OnResult_ThrowsException
            extends OnResult {
        public abstract void onError(Exception e);
    }

    static public abstract class OnSelect {
        public abstract void onError(Exception e);
        public abstract void onResult(List<JSONArray> rows);
    }

    static public abstract class OnTableColumnInfos {
        public abstract void onError(Exception e);
        public abstract void onResult(ColumnInfo[] columnInfos);
    }

    static public abstract class OnTableNames {
        public abstract void onError(Exception e);
        public abstract void onResult(String[] tableNames);
    }

}
