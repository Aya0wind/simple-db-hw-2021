package simpledb.execution;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

class StringGroupAggregator implements Aggregator {
    HashMap<Field, AggregatePair<String>> groupMap;
    Type gbfieldtype;
    int gbfield;
    int afield;
    Op what;

    public StringGroupAggregator(Type gbfieldtype, Aggregator.Op what, int afield, int gbfield) {
        this.groupMap = new HashMap<>();
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.afield = afield;
        this.gbfield = gbfield;
    }

    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = tup.getField(gbfield);
        AggregatePair<String> value = groupMap.getOrDefault(groupField,
                AggregatePair.takeInitialValueByOp(what, String.class));
        switch (what) {
            case COUNT:
                value.setCounter(value.getCounter() + 1);
                groupMap.put(groupField, value);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public OpIterator iterator() {
        return new OpIterator() {
            Iterator<Entry<Field, AggregatePair<String>>> iterator = groupMap.entrySet().iterator();
            boolean closed = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                closed = false;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return !closed && iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (closed)
                    throw new NoSuchElementException();
                Entry<Field, AggregatePair<String>> nextValue = iterator.next();
                Tuple tuple = new Tuple(getTupleDesc());
                switch (what) {
                    case COUNT:
                        tuple.setField(gbfield, nextValue.getKey());
                        tuple.setField(afield, new IntField(nextValue.getValue().getCounter()));
                        return tuple;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iterator = groupMap.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
            }

            @Override
            public void close() {
                closed = true;
            }

        };
    }
}

class StringNoGroupAggregator implements Aggregator {
    AggregatePair<String> aggreagateResult;
    int gbfield;
    int afield;
    Op what;

    public StringNoGroupAggregator(Aggregator.Op what, int afield) {
        this.aggreagateResult = AggregatePair.takeInitialValueByOp(what, String.class);
        this.what = what;
        this.afield = afield;
    }

    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        switch (what) {
            case COUNT:
                aggreagateResult.setCounter(aggreagateResult.getCounter() + 1);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public OpIterator iterator() {
        return new OpIterator() {
            boolean closed = true;
            boolean readed = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                closed = false;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return !closed && !readed;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (closed)
                    throw new NoSuchElementException();
                Tuple tuple = new Tuple(getTupleDesc());
                switch (what) {
                    case COUNT:
                        tuple.setField(0, new IntField(aggreagateResult.getCounter()));
                        return tuple;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                readed = false;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return new TupleDesc(new Type[] { Type.INT_TYPE });
            }

            @Override
            public void close() {
                closed = true;
            }

        };
    }
}

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private Aggregator inner;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            inner = new StringNoGroupAggregator(what, afield);
        } else {
            inner = new StringGroupAggregator(gbfieldtype, what, afield, gbfield);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        inner.mergeTupleIntoGroup(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return inner.iterator();
    }

}
