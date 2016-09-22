package daisy;

import aggregate.TimeBasedSingleWindow;

public class DaisyWindow implements
		TimeBasedSingleWindow<InputTuple, OutputTuple> {

	private double count;
	private double sum;
	private String key;

	public DaisyWindow() {
	}

	@Override
	public TimeBasedSingleWindow<InputTuple, OutputTuple> factory() {
		return new DaisyWindow();
	}

	@Override
	public void setup() {
		count = 0;
		sum = 0;
		key = null;
	}

	@Override
	public void add(InputTuple t) {
		sum += Math.max((t.getSpeedLimit() - t.getSpeed()) / t.getSpeedLimit(),
				0.0);
		count++;
		if (key == null)
			key = t.getKey();
		else if (!key.equals(t.getKey()))
			throw new RuntimeException("Key changed for window");
	}

	@Override
	public void remove(InputTuple t) {
		sum -= Math.max((t.getSpeedLimit() - t.getSpeed()) / t.getSpeedLimit(),
				0.0);
		count--;
	}

	@Override
	public OutputTuple getAggregatedResult(double timestamp,
			InputTuple triggeringTuple) {

		return new OutputTuple(timestamp, key, sum / count, (int) count);
	}

	@Override
	public long getNumberOfTuples() {
		return (long) count;
	}

}
