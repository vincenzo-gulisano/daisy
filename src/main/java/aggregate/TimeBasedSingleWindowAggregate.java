package aggregate;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

public class TimeBasedSingleWindowAggregate<T_IN extends DaisyTuple, T_OUT extends DaisyTuple> {

	// USER PARAMETERS
	private double WS;
	private double WA;
	private TimeBasedSingleWindow<T_IN, T_OUT> aggregateWindow;

	// OTHERS
	private double latestTimestamp;
	private boolean firstTuple = true;

	LinkedList<T_IN> tuples;
	TreeMap<Double, HashMap<String, TimeBasedSingleWindow<T_IN, T_OUT>>> windows;

	public TimeBasedSingleWindowAggregate(double WS, double WA,
			TimeBasedSingleWindow<T_IN, T_OUT> aggregateWindow) {
		tuples = new LinkedList<T_IN>();
		windows = new TreeMap<Double, HashMap<String, TimeBasedSingleWindow<T_IN, T_OUT>>>();
		this.WS = WS;
		this.WA = WA;
		this.aggregateWindow = aggregateWindow;
	}

	public double getEarliestWinStartTS(double ts) {
		return Math.max((Math.floor(ts / WA) - Math.ceil(WS / WA) + 1) * WA,
				0.0);
	}

	public List<T_OUT> processTuple(T_IN t) {

		List<T_OUT> result = new LinkedList<T_OUT>();

		// make sure timestamps are not decreasing
		if (firstTuple) {
			firstTuple = false;
		} else {
			if (t.getTS() < latestTimestamp) {
				throw new RuntimeException("Input tuple's timestamp decreased!");
			}
		}
		latestTimestamp = t.getTS();

		double earliestWinStartTSforT = getEarliestWinStartTS(t.getTS());

		// Managing of stale windows
		boolean purgingNotDone = true;
		while (purgingNotDone && windows.size() > 0) {

			double earliestWinStartTS = windows.firstKey();

			if (earliestWinStartTS < earliestWinStartTSforT) {

				// Produce results for stale windows
				for (TimeBasedSingleWindow<T_IN, T_OUT> w : windows.get(
						earliestWinStartTS).values()) {
					result.add(w.getAggregatedResult(earliestWinStartTS, t));
				}

				// Remove contribution of stale tuples from stale windows
				while (tuples.size() > 0) {
					T_IN tuple = tuples.peek();
					if (tuple.getTS() < earliestWinStartTS + WA) {

						windows.get(earliestWinStartTS).get(tuple.getKey())
								.remove(tuple);

						if (windows.get(earliestWinStartTS).get(tuple.getKey())
								.getNumberOfTuples() == 0)
							windows.get(earliestWinStartTS).remove(
									tuple.getKey());
						tuples.pop();

					} else {
						break;
					}
				}

				// Shift windows
				if (!windows.containsKey(earliestWinStartTS + WA))
					windows.put(
							earliestWinStartTS + WA,
							new HashMap<String, TimeBasedSingleWindow<T_IN, T_OUT>>());
				windows.get(earliestWinStartTS + WA).putAll(
						windows.get(earliestWinStartTS));
				windows.remove(earliestWinStartTS);

			} else {
				purgingNotDone = false;
			}
		}

		// Add contribution of this tuple
		if (!windows.containsKey(earliestWinStartTSforT))
			windows.put(earliestWinStartTSforT,
					new HashMap<String, TimeBasedSingleWindow<T_IN, T_OUT>>());
		if (!windows.get(earliestWinStartTSforT).containsKey(t.getKey()))
			windows.get(earliestWinStartTSforT).put(t.getKey(),
					aggregateWindow.factory());
		windows.get(earliestWinStartTSforT).get(t.getKey()).add(t);

		// Store tuple
		tuples.add(t);

		return result;

	}
}
