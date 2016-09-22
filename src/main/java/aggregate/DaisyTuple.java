package aggregate;

public class DaisyTuple {

	private double ts;
	private String key;

	@SuppressWarnings("unused")
	private DaisyTuple() {

	}

	public DaisyTuple(double ts, String key) {
		this.ts = ts;
		this.key = key;
	}

	public double getTS() {
		return ts;
	}

	public String getKey() {
		return key;
	}

}
