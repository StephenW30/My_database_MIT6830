package simpledb.storage;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

public class PageLock {
    public static int SHARE_LOCK = 0;        // share lock
    public static int EXCLUSIVE_LOCK = 1;    // exclusive lock
    private int type;                        // type of the lock
    private TransactionId transactionId;;   // transaction id

    public PageLock(int type, TransactionId transactionId){
        this.transactionId = transactionId;
        this.type = type;
    }

    public int getType(){
        return type;
    }

    public void setType(int type){
        this.type = type;
    }

    public TransactionId getTransactionId(){
        return transactionId;
    }

    @Override
    public String toString(){
        String str = "PageLock{" + "type=" + type + ", transactionId=" + transactionId + "}";
        return str;
    }

}
