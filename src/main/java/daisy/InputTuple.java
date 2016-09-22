package daisy;

import aggregate.DaisyTuple;

public class InputTuple extends DaisyTuple {

	private double speed;
	private double speedLimit;

	public InputTuple(double ts, String segment, double speed, double speedLimit) {
		super(ts, segment);
		this.speed = speed;
		this.speedLimit = speedLimit;
	}

	public double getSpeed() {
		return speed;
	}

	public double getSpeedLimit() {
		return speedLimit;
	}

}
