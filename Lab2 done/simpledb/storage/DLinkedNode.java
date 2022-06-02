package simpledb.storage;
import simpledb.storage.PageId;

public class DLinkedNode {
    PageId value;
    DLinkedNode prev;
    DLinkedNode next;
    public DLinkedNode() {}

    public DLinkedNode(PageId value) {
        this.value = value;
    }

    public PageId getValue() {
        return value;
    }
}
