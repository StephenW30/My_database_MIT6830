package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleDesc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private static final Field NO_GROUP = new IntField(-1);

    private int gbfield;
    private Type gbfieldType;

    private int afield;
    private Op what;

    private TupleDesc tupleDesc;
    private Map<Field, Tuple> aggregate;

    private int counts;
    private int summary;

    private Map<Field, Integer> countsMap;
    private Map<Field, Integer> sumMap;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregate = new ConcurrentHashMap<>();
        if (gbfield == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateValue"});
            Tuple tuple = new Tuple(tupleDesc);
            this.aggregate.put(NO_GROUP, tuple);
        }
        else {
            this.tupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {"groupValue", "aggregateValue"});
        }
        if (gbfield == NO_GROUPING && what.equals(Op.AVG)) {
            this.counts = 0;
            this.summary = 0;
        }
        else if (gbfield != NO_GROUPING && what.equals(Op.AVG)) {
            this.countsMap = new ConcurrentHashMap<>();
            this.sumMap = new ConcurrentHashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField operationField = (IntField) tup.getField(afield);
        if (operationField == null) {
            return;
        }
        if (gbfield == NO_GROUPING) {
            Tuple tuple = aggregate.get(NO_GROUP);
            IntField field = (IntField) tuple.getField(0);
            if (field == null) {
                if (what.equals(Op.COUNT)) {
                    tuple.setField(0, new IntField(1));
                } else if (what.equals(Op.AVG)) {
                    counts++;
                    summary = operationField.getValue();
                    tuple.setField(0, operationField);
                } else {
                    tuple.setField(0, operationField);
                }
                return;
            }
            switch (what) {
                case MIN:
                    if (operationField.compare(Predicate.Op.LESS_THAN, field)) {
                        tuple.setField(0, operationField);
                        aggregate.put(NO_GROUP, tuple);
                    }
                    return;
                case MAX:
                    if (operationField.compare(Predicate.Op.GREATER_THAN, field)) {
                        tuple.setField(0, operationField);
                        aggregate.put(NO_GROUP, tuple);
                    }
                    return;
                case COUNT:
                    IntField count = new IntField(field.getValue() + 1);
                    tuple.setField(0, count);
                    aggregate.put(NO_GROUP, tuple);
                    return;
                case SUM:
                    IntField sum = new IntField(field.getValue() + operationField.getValue());
                    tuple.setField(0, sum);
                    aggregate.put(NO_GROUP, tuple);
                    return;
                case AVG:
                    counts++;
                    summary += operationField.getValue();
                    IntField avg = new IntField(summary / counts);
                    tuple.setField(0, avg);
                    aggregate.put(NO_GROUP, tuple);
                    return;
                default:
                    return;
            }
        } else {
            Field groupField = tup.getField(gbfield);
            if (!aggregate.containsKey(groupField)) {
                Tuple value = new Tuple(this.tupleDesc);
                value.setField(0, groupField);
                if (what.equals(Op.COUNT)) {
                    value.setField(1, new IntField(1));
                } else if (what.equals(Op.AVG)) {
                    countsMap.put(groupField, countsMap.getOrDefault(groupField, 0) + 1);
                    sumMap.put(groupField, sumMap.getOrDefault(groupField, 0) + operationField.getValue());
                    value.setField(1, operationField);
                } else {
                    value.setField(1, operationField);
                }
                aggregate.put(groupField, value);
                return;
            }
            Tuple tuple = aggregate.get(groupField);
            IntField field = (IntField) tuple.getField(1);
            switch (what) {
                case MIN:
                    if (operationField.compare(Predicate.Op.LESS_THAN, field)) {
                        tuple.setField(1, operationField);
                        aggregate.put(groupField, tuple);
                    }
                    return;
                case MAX:
                    if (operationField.compare(Predicate.Op.GREATER_THAN, field)) {
                        tuple.setField(1, operationField);
                        aggregate.put(groupField, tuple);
                    }
                    return;
                case COUNT:
                    IntField count = new IntField(field.getValue() + 1);
                    tuple.setField(1, count);
                    aggregate.put(groupField, tuple);
                    return;
                case SUM:
                    IntField sum = new IntField(field.getValue() + operationField.getValue());
                    tuple.setField(1, sum);
                    aggregate.put(groupField, tuple);
                    return;
                case AVG:
                    countsMap.put(groupField, countsMap.getOrDefault(groupField, 0) + 1);
                    sumMap.put(groupField, sumMap.getOrDefault(groupField, 0) + operationField.getValue());
                    IntField avg = new IntField(sumMap.get(groupField) / countsMap.get(groupField));
                    tuple.setField(1, avg);
                    aggregate.put(groupField, tuple);
                    return;
                default:
                    return;
            }
        }
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }
    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new
        //        UnsupportedOperationException("please implement me for lab2");
        return new IntOpIterator(this);
    }
    public class IntOpIterator implements OpIterator {
        private Iterator<Tuple> iterator;
        private IntegerAggregator aggregator;

        public IntOpIterator(IntegerAggregator aggregator) {
            this.aggregator = aggregator;
            this.iterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}