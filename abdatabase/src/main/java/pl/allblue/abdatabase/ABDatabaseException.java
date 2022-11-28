package pl.allblue.abdatabase;

public class ABDatabaseException extends Exception {

    public ABDatabaseException(String message)
    {
        super(message);
    }

    public ABDatabaseException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
