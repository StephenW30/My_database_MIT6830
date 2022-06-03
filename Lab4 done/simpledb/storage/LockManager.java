package simpledb.storage;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

public class LockManager {
    private Map<PageId, Map<TransactionId, PageLock>> pageLockMap;
    public LockManager(){
        pageLockMap = new ConcurrentHashMap<PageId, Map<TransactionId, PageLock>>();
    }

    /**
     *  using LockManager to implement the function of the management of the Lock
     *  3 main features are implemented in this class: acquire lock, release lock
     *  and enquire the specific page's lock status.
     *
     *  Here is the logic of adding lock.
     *  if there is no lock in LockManager || this page has no lock inside
     *      add lock directly.(both write lock and read lock)
     *  else
     *      if (transaction has lock in the page)
     *          if (is adding read lock)
     *              add read lock directly
     *          else if (is adding write lock)
     *              if (the number of lock is 1)
     *                  upgrade the level of lock
     *              if (the number of lock is >1)
     *                  deadlock happens
     *      if (transaction has no lock in the page)
     *          if (is adding read lock)
     *              if (the number of lock is 1 and is read lock)
     *                  add the lock directly.
     *              if (the number of lock is 1 and is write lock)
     *                  wait.
     *              if (the number of lock is >1) // which implicates there more than one read lock
     *                  add the lock directly
     *          if (is adding write lock)
     *              wait.
     *
    **/

    public synchronized boolean acquireLock(
            PageId pageId,
            TransactionId transactionId,
            int acquireType
    ) throws TransactionAbortedException, InterruptedException {
        String lockType = (acquireType==0) ? "Read lock" : "Write lock";
        String threadName = Thread.currentThread().getName();

        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pageId);
        if(lockMap == null || lockMap.size() == 0){
            PageLock pageLock = new PageLock(acquireType,transactionId);
            lockMap = new ConcurrentHashMap<>();
            lockMap.put(transactionId,pageLock);
            pageLockMap.put(pageId,lockMap);
            System.out.println(threadName + ": the" + pageId + " have no lock,transaction " +
                    transactionId + " require" + lockType + "success");
            return true;
        }

        PageLock lock = lockMap.get(transactionId);

        if(lock != null){
            if (acquireType == PageLock.SHARE_LOCK){
                System.out.println(threadName + ": the" + pageId + " have a read lock in this transactionId," +
                        "transaction " + transactionId + "require" + lockType + "success");
                return true;
            }
            if (acquireType == PageLock.EXCLUSIVE_LOCK){
                if (lockMap.size()>1){
                    System.out.println(threadName + ": the" + pageId + " have many read locks in this transactionId," +
                            "transaction " + transactionId + "require" + lockType + "fail");
                    throw new TransactionAbortedException();
                }
                if (lockMap.size() == 1 && lock.getType() == PageLock.SHARE_LOCK){
                    lock.setType(PageLock.EXCLUSIVE_LOCK);
                    lockMap.put(transactionId, lock);
                    pageLockMap.put(pageId, lockMap);
                    System.out.println(threadName + ": the" + pageId + " have a read lock in this transactionId," +
                            "transaction " + transactionId + "require" + lockType + "success and upgrade");
                    return true;
                }
                if (lockMap.size() == 1 && lock.getType() == PageLock.EXCLUSIVE_LOCK){
                    System.out.println(threadName + ": the" + pageId + " have write lock in this transactionId," +
                            "transaction " + transactionId + "require" + lockType + "success");
                    return true;
                }
            }
        }

        if(lock == null){
            if (acquireType == PageLock.SHARE_LOCK){
                if (lockMap.size() > 1){
                    PageLock pageLock = new PageLock(acquireType, transactionId);
                    lockMap.put(transactionId, pageLock);
                    pageLockMap.put(pageId, lockMap);
                    System.out.println(threadName + ": the" + pageId + " have many read locks in this transactionId," +
                            "transaction " + transactionId + "require" + lockType + "success");
                    return true;
                }
                PageLock l = null;
                for (PageLock value : lockMap.values()){
                    l = value;
                }
                if (lockMap.size() == 1 && l.getType() == PageLock.SHARE_LOCK){
                    PageLock pageLock = new PageLock(acquireType, transactionId);
                    lockMap.put(transactionId, pageLock);
                    pageLockMap.put(pageId, lockMap);
                    System.out.println(threadName + ": the" + pageId + " have one lock in this transactionId," +
                            "transaction " + transactionId + "require" + lockType + "success");
                    return true;
                }
                if (lockMap.size() == 1 && l.getType() == PageLock.EXCLUSIVE_LOCK){
                    wait(50);
                    return false;
                }
            }
            if (acquireType == PageLock.EXCLUSIVE_LOCK){
                wait(10);
                return false;
            }
        }
        return true;
    }

    /**
     * release lock.
     * @param   pageId
     * @param   tid
     */
    public synchronized void releaseLock(PageId pageId, TransactionId tid){
        final String threadName = Thread.currentThread().getName();

        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pageId);
        if (lockMap == null) {
            return;
        }
        if (tid == null) {
            return;
        }
        PageLock lock = lockMap.get(tid);
        if (lock == null) {
            return;
        }
        final String lockType = lockMap.get(tid).getType() == 0 ? "Read lock" : "Write lock";
        lockMap.remove(tid);

        System.out.println(threadName + " release " + lockType + " in " + pageId + ", the tid lock size is " + lockMap.size());
        if (lockMap.size() == 0) {
            pageLockMap.remove(pageId);
            System.out.println(threadName + "release last lock, the page " + pageId + " have no lock, the page locks size is " + pageLockMap.size());
        }
        this.notifyAll();
    }

    public synchronized boolean isHoldLock(PageId pageId, TransactionId tid){
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pageId);
        if(lockMap == null){
            return false;
        }
        return lockMap.get(tid) != null;
    }

    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> pageIds = pageLockMap.keySet();
        for (PageId pageId : pageIds) {
            releaseLock(pageId, tid);
        }
    }

}
