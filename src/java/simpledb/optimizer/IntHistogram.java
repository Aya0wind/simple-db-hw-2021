package simpledb.optimizer;

import java.util.SortedSet;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {
    private int min;
    private int max;
    private double bucketRange;
    private int[] histogram;
    private int recordNum;

    /**
     * @return the min
     */
    public int minVal() {
        return min;
    }

    /**
     * @return the max
     */
    public int maxVal() {
        return max;
    }

    /**
     * @return the recordNum
     */
    public int recordNum() {
        return recordNum;
    }

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it
     * receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through
     * the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed. For
     * example, you shouldn't
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this
     *                class for histogramming
     * @param max     The maximum integer value that will ever be passed to this
     *                class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        double range = max - min + 1.0;
        this.histogram = new int[buckets];
        this.min = min;
        this.max = max;
        this.bucketRange = range / buckets;
        this.recordNum = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v <= max && v >= min) {
            int bucketIndex = findBucketIndex(v);
            histogram[bucketIndex]++;
            recordNum++;
        } else {
            throw new IllegalArgumentException("value out of range");
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS:
                return estimateEquals(v);
            case GREATER_THAN:
                return estimateGreaterThan(v);
            case GREATER_THAN_OR_EQ:
                return estimateGreaterThanOrEqual(v);
            case LESS_THAN:
                return estimateLessThan(v);
            case LESS_THAN_OR_EQ:
                return estimateLessThanOrEqual(v);
            case NOT_EQUALS:
                return estimateNotEquals(v);
            default:
                return -1.0;
        }
        // some code goes here
    }

    private int findBucketIndex(int v) {
        return (int) ((v - min) / bucketRange);
    }

    private double estimateNotEquals(int v) {
        return 1.0 - estimateEquals(v);
    }

    private double estimateLessThanOrEqual(int v) {
        return estimateLessThan(v + 1);
    }

    private double estimateLessThan(int v) {
        if (v <= min) {
            return 0;
        } else if (v >= max) {
            return 1;
        }
        int bucketIndex = findBucketIndex(v);
        double others = 0;
        for (int i = 0; i < bucketIndex; i++) {
            others += histogram[i];
        }
        others += (histogram[bucketIndex] / bucketRange) * (v - min - bucketIndex * bucketRange);
        return others / recordNum;
    }

    private double estimateGreaterThanOrEqual(int v) {
        return estimateGreaterThan(v - 1);
    }

    private double estimateGreaterThan(int v) {
        return 1.0 - estimateLessThan(v + 1);
    }

    private double estimateEquals(int v) {
        return estimateLessThan(v + 1) - estimateLessThan(v);
    }

    /**
     * @return
     *         the average selectivity of this histogram.
     * 
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < histogram.length; i++) {
            sb.append(histogram[i]);
            sb.append(" ");
        }
        return sb.toString();
    }
}
