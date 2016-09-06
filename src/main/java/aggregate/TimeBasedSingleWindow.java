package aggregate;

public interface TimeBasedSingleWindow<T_IN, T_OUT> {

	public TimeBasedSingleWindow<T_IN, T_OUT> factory();

	public void setup();

	public void add(T_IN t);

	public void remove(T_IN t);

	public T_OUT getAggregatedResult(long timestamp, String groupby,
			T_IN triggeringTuple);

	public long getTimestamp(T_IN t);

	public String getKey(T_IN t);

}
