package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private DbFile file;
    private Object[] histograms;
    private int ioCostPerPage;
    private int pageNum;
    private TupleDesc td;
    private int totalTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *                      The table over which to compute statistics
     * @param ioCostPerPage
     *                      The cost per page of IO. This doesn't differentiate
     *                      between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.td = file.getTupleDesc();
        this.pageNum = ((HeapFile) file).numPages();
        this.ioCostPerPage = ioCostPerPage;
        int numFields = td.numFields();
        this.histograms = new Object[numFields];
        DbFileIterator iterator = file.iterator(new TransactionId());
        
        try {
            iterator.open();
            MinMax[] fieldMinMax = initializeHistogram(iterator);
            iterator.rewind();
            buildHistogram(iterator, numFields, fieldMinMax);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            iterator.close();
        }

        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    }

    class MinMax {
        public int min;
        public int max;
    }

    private MinMax[] initializeHistogram(DbFileIterator iterator)
            throws NoSuchElementException, DbException, TransactionAbortedException {
        int numFields = td.numFields();
        MinMax[] fieldMinMax = new MinMax[numFields];
        // 构建每个字段的Histogram min max，保存在fieldMinMax中
        for (int i = 0; i < numFields; i++) {
            switch (td.getFieldType(i)) {
                case INT_TYPE:
                    MinMax integerMinMax = new MinMax();
                    integerMinMax.min = Integer.MAX_VALUE;
                    integerMinMax.max = Integer.MIN_VALUE;
                    fieldMinMax[i] = integerMinMax;
                    break;
                case STRING_TYPE:
                    MinMax stringMinMax = new MinMax();
                    stringMinMax.min = StringHistogram.minVal();
                    stringMinMax.max = StringHistogram.maxVal();
                    fieldMinMax[i] = stringMinMax;
                    break;
                default:
                    throw new RuntimeException("Unsupported field type");
            }
        }
        //第一次遍历，计算min max 和总的tuple数
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            totalTuples++;
            for (int i = 0; i < numFields; i++) {
                switch (td.getFieldType(i)) {
                    case INT_TYPE:
                        MinMax integerMinMax = (MinMax) fieldMinMax[i];
                        integerMinMax.min = Integer.min(integerMinMax.min, ((IntField) t.getField(i)).getValue());
                        integerMinMax.max = Integer.max(integerMinMax.max, ((IntField) t.getField(i)).getValue());
                        break;
                    case STRING_TYPE:
                        break;
                    default:
                        throw new RuntimeException("Unsupported field type");
                }
            }
        }
        return fieldMinMax;
    }

    private void buildHistogram(DbFileIterator iterator, int numFields, Object[] fieldMinMax) {
        for (int i = 0; i < td.numFields(); i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                MinMax minMax = (MinMax) fieldMinMax[i];
                histograms[i] = new IntHistogram(NUM_HIST_BINS, minMax.min, minMax.max);
            } else {
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }
        try {
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = tuple.getField(i);
                    if (field.getType() == Type.INT_TYPE) {
                        IntField intField = (IntField) field;
                        IntHistogram intHistogram = (IntHistogram) histograms[i];
                        intHistogram.addValue(intField.getValue());
                    } else if (field.getType() == Type.STRING_TYPE) {
                        StringField stringField = (StringField) field;
                        StringHistogram stringHistogram = (StringHistogram) histograms[i];
                        if (stringHistogram == null) {
                            stringHistogram = new StringHistogram(NUM_HIST_BINS);
                            histograms[i] = stringHistogram;
                        }
                        stringHistogram.addValue(stringField.getValue());
                    } else {
                        throw new RuntimeException("Unsupported field type");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return pageNum * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *                          The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * 
     * @param field
     *              the index of the field
     * @param op
     *              the operator in the predicate
     *              The semantic of the method is that, given the table, and then
     *              given a
     *              tuple, of which we do not know the value of the field, return
     *              the
     *              expected selectivity. You may estimate this value from the
     *              histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        Type fieldType = td.getFieldType(field);
        switch (fieldType) {
            case INT_TYPE:
                IntHistogram intHistogram = (IntHistogram) histograms[field];
                return intHistogram.avgSelectivity();
            case STRING_TYPE:
                StringHistogram stringHistogram = (StringHistogram) histograms[field];
                return stringHistogram.avgSelectivity();
            default:
                throw new RuntimeException("Unsupported field type");
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *                 The field over which the predicate ranges
     * @param op
     *                 The logical operation in the predicate
     * @param constant
     *                 The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = td.getFieldType(field);
        switch (fieldType) {
            case INT_TYPE:
                IntHistogram intHistogram = (IntHistogram) histograms[field];
                IntField intField = (IntField) constant;
                return intHistogram.estimateSelectivity(op, intField.getValue());
            case STRING_TYPE:
                StringHistogram stringHistogram = (StringHistogram) histograms[field];
                StringField stringField = (StringField) constant;
                return stringHistogram.estimateSelectivity(op, stringField.getValue());
            default:
                throw new RuntimeException("Unsupported field type");
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return totalTuples;
    }

}
