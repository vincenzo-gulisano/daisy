package aggregate;

/* Assumptions: (1) all tuples added to an instance of such a 
 * window share the same key and (2) it is the responsibility 
 * of the window to set the key in the output tuple if needed */
public interface TimeBasedSingleWindow<T_IN extends DaisyTuple, T_OUT extends DaisyTuple> {

	public TimeBasedSingleWindow<T_IN, T_OUT> factory();

	public void setup();

	public void add(T_IN t);

	public void remove(T_IN t);

	public T_OUT getAggregatedResult(double timestamp, T_IN triggeringTuple);

	public long getNumberOfTuples();

}
