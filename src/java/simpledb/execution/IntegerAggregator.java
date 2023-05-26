package simpledb.execution;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

class IntegerGroupAggregator implements Aggregator {
    HashMap<Field, AggregatePair<Integer>> groupMap;
    Type gbfieldtype;
    int gbfield;
    int afield;
    Op what;

    public IntegerGroupAggregator(Type gbfieldtype, Aggregator.Op what, int afield, int gbfield) {
        this.groupMap = new HashMap<>();
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.afield = afield;
        this.gbfield = gbfield;
    }

    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field aggregatField = tup.getField(afield);
        Field groupField = tup.getField(gbfield);
        AggregatePair<Integer> value = groupMap.getOrDefault(groupField,
                AggregatePair.takeInitialValueByOp(what, Integer.class));
        switch (what) {
            case AVG: {
                value.setValue(value.getValue() + ((IntField) aggregatField).getValue());
                value.setCounter(value.getCounter() + 1);
                groupMap.put(groupField, value);
                break;
            }
            case COUNT:
                value.setCounter(value.getCounter() + 1);
                groupMap.put(groupField, value);
                break;
            case MAX:
                value.setValue(Integer.max(value.getValue(), ((IntField) aggregatField).getValue()));
                groupMap.put(groupField, value);
                break;
            case MIN:
                value.setValue(Integer.min(value.getValue(), ((IntField) aggregatField).getValue()));
                groupMap.put(groupField, value);
                break;
            case SUM:
                value.setValue(value.getValue() + ((IntField) aggregatField).getValue());
                groupMap.put(groupField, value);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public OpIterator iterator() {
        return new OpIterator() {
            Iterator<Entry<Field, AggregatePair<Integer>>> iterator = groupMap.entrySet().iterator();
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
                Entry<Field, AggregatePair<Integer>> nextValue = iterator.next();
                Tuple tuple = new Tuple(getTupleDesc());
                switch (what) {
                    case AVG: {
                        tuple.setField(gbfield, nextValue.getKey());
                        tuple.setField(afield,
                                new IntField(nextValue.getValue().getValue() / nextValue.getValue().getCounter()));
                        return tuple;
                    }
                    case COUNT:
                        tuple.setField(gbfield, nextValue.getKey());
                        tuple.setField(afield, new IntField(nextValue.getValue().getCounter()));
                        return tuple;
                    case MAX:
                        tuple.setField(gbfield, nextValue.getKey());
                        tuple.setField(afield, new IntField(nextValue.getValue().getValue()));
                        return tuple;
                    case MIN:
                        tuple.setField(gbfield, nextValue.getKey());
                        tuple.setField(afield, new IntField(nextValue.getValue().getValue()));
                        return tuple;
                    case SUM:
                        tuple.setField(gbfield, nextValue.getKey());
                        tuple.setField(afield, new IntField(nextValue.getValue().getValue()));
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

class IntegerNoGroupAggregator implements Aggregator {
    AggregatePair<Integer> aggreagateResult;
    int gbfield;
    int afield;
    Op what;

    public IntegerNoGroupAggregator(Aggregator.Op what, int afield) {
        this.aggreagateResult = AggregatePair.takeInitialValueByOp(what, Integer.class);
        this.what = what;
        this.afield = afield;
    }

    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field aggregatField = tup.getField(afield);
        switch (what) {
            case AVG: {
                aggreagateResult.setValue(aggreagateResult.getValue() + ((IntField) aggregatField).getValue());
                aggreagateResult.setCounter(aggreagateResult.getCounter() + 1);
                break;
            }
            case COUNT:
                aggreagateResult.setCounter(aggreagateResult.getCounter() + 1);
                break;
            case MAX:
                aggreagateResult.setValue(Integer.max(
                        aggreagateResult.getValue(), ((IntField) aggregatField).getValue()));
                break;
            case MIN:
                aggreagateResult.setValue(Integer.min(
                        aggreagateResult.getValue(), ((IntField) aggregatField).getValue()));
                break;
            case SUM:
                aggreagateResult.setValue(aggreagateResult.getValue() + ((IntField) aggregatField).getValue());
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public OpIterator iterator() {
        return new OpIterator() {
            boolean closed = false;
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
                readed = true;
                switch (what) {
                    case AVG: {
                        tuple.setField(0, new IntField(
                                aggreagateResult.getValue() / aggreagateResult.getCounter()));
                        return tuple;
                    }
                    case COUNT:
                        tuple.setField(0, new IntField(aggreagateResult.getCounter()));
                        return tuple;
                    case MAX:
                        tuple.setField(0, new IntField(aggreagateResult.getValue()));
                        return tuple;
                    case MIN:
                        tuple.setField(0, new IntField(aggreagateResult.getValue()));
                        return tuple;
                    case SUM:
                        tuple.setField(0, new IntField(aggreagateResult.getValue()));
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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private Aggregator inner;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            inner = new IntegerNoGroupAggregator(what, afield);
        } else {
            inner = new IntegerGroupAggregator(gbfieldtype, what, afield, gbfield);
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
        inner.mergeTupleIntoGroup(tup);
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
        return inner.iterator();
    }

}
