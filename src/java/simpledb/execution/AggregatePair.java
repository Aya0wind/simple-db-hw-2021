package simpledb.execution;

public class AggregatePair<T> {

    public static <T> AggregatePair<T> takeInitialValueByOp(Aggregator.Op what, Class<T> type) {
        if (type == Integer.class) {
            if (what == Aggregator.Op.MIN) {
                return new AggregatePair<T>(Integer.valueOf(Integer.MAX_VALUE), 0);
            } else if (what == Aggregator.Op.MAX) {
                return new AggregatePair<T>(Integer.valueOf(Integer.MIN_VALUE), 0);
            } else {
                return new AggregatePair<T>(Integer.valueOf(0), 0);
            }
        } else {
            return new AggregatePair<T>("", 0);
        }

    }

    private T value;
    private int counter;

    /**
     * @param value
     * @param counter
     */
    public AggregatePair(Integer value, int counter) {
        this.value = (T) value;
        this.counter = counter;
    }

    /**
     * @param value
     * @param counter
     */
    public AggregatePair(String value, int counter) {
        this.value = (T) value;
        this.counter = counter;
    }

    /**
     * @return the value
     */
    public T getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(T value) {
        this.value = value;
    }

    /**
     * @return the counter
     */
    public int getCounter() {
        return counter;
    }

    /**
     * @param counter the counter to set
     */
    public void setCounter(int counter) {
        this.counter = counter;
    }

}
