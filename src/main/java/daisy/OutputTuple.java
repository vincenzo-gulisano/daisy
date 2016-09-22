package daisy;

import aggregate.DaisyTuple;

public class OutputTuple extends DaisyTuple {

	private double avgCongestion;
	private int contributingTuples;

	public int getContributingTuples() {
		return contributingTuples;
	}

	public OutputTuple(double ts, String segment, double avgCongestion,
			int contributingTuples) {
		super(ts, segment);
		this.avgCongestion = avgCongestion;
		this.contributingTuples = contributingTuples;
	}

	public double getAvgCongestion() {
		return avgCongestion;
	}

}
