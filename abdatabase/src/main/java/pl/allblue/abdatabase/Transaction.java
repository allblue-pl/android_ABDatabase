package pl.allblue.abdatabase;

public class Transaction {

    static public abstract class OnFinish {
        public abstract void onError(Exception e);
        public abstract void onResult();
    }

    static public abstract class OnIsAutocommit {
        public abstract void onError(Exception e);
        public abstract void onResult(Integer transactionId);
    }

    static public abstract class OnStart {
        public abstract void onError(Exception e);
        public abstract void onResult(int transactionId);
    }

}
