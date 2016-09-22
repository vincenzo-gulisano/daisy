package daisy;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stats.CountStat;
import aggregate.TimeBasedSingleWindowAggregate;

public class Daisy {

	static Logger LOG = LoggerFactory.getLogger(Daisy.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();

		BasicConfigurator.configure();

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		SingleOutputStreamOperator<String> in = env
				.readTextFile(params.getRequired("inputFile"))
				.setParallelism(1).name("input");

		SingleOutputStreamOperator<InputTuple> conv = in
				.flatMap(new RichFlatMapFunction<String, InputTuple>() {

					boolean header;
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");

					private CountStat stat;
					private long interval = 30;

					public void open(Configuration parameters) throws Exception {

						header = true;

						ParameterTool params = (ParameterTool) getRuntimeContext()
								.getExecutionConfig().getGlobalJobParameters();

						sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

						LOG.info("created throughput statistic at  "
								+ params.getRequired("throughputStatFile"));
						stat = new CountStat("", params
								.getRequired("throughputStatFile"), true);

					}

					@Override
					public void close() throws Exception {
						stat.writeStats();
					}

					public void flatMap(String value, Collector<InputTuple> out)
							throws Exception {

						if (header) {
							header = false;
							return;
						}

						long ts = -1;
						try {

							String tsString = value.split(",")[5];
							ts = sdf.parse(tsString).getTime();
							sdf.applyPattern("HH");
							long hour = Long.valueOf(sdf.format(new Date(ts)));
							sdf.applyPattern("mm");
							long minute_stump = Long.valueOf(sdf
									.format(new Date(ts))) / interval;
							sdf.applyPattern("yyyy-MM-dd HH:mm:ss");
							String intervalTime = hour + "-" + minute_stump;

							String key = value.split(",")[0] + ","
									+ value.split(",")[1] + ","
									+ value.split(",")[2] + "," + intervalTime;
							double speedLimit = Double.valueOf(value.split(",")[3]);
							double speed = Double.valueOf(value.split(",")[4]);

							stat.increase(1);

							out.collect(new InputTuple(ts, key, speed,
									speedLimit));
						} catch (Exception e) {
							LOG.warn("Cannot convert input string " + value);
							throw e;
						}

					}
				}).setParallelism(1).name("converter");

		SingleOutputStreamOperator<OutputTuple> outStream = conv
				.flatMap(new RichFlatMapFunction<InputTuple, OutputTuple>() {

					TimeBasedSingleWindowAggregate<InputTuple, OutputTuple> agg;

					public void open(Configuration parameters) throws Exception {

						ParameterTool params = (ParameterTool) getRuntimeContext()
								.getExecutionConfig().getGlobalJobParameters();

						agg = new TimeBasedSingleWindowAggregate<InputTuple, OutputTuple>(
								Double.valueOf(params.getRequired("WS")),
								Double.valueOf(params.getRequired("WA")),
								new DaisyWindow());

					}

					@Override
					public void close() throws Exception {
					}

					public void flatMap(InputTuple t, Collector<OutputTuple> out)
							throws Exception {

						List<OutputTuple> outTuples = agg.processTuple(t);
						for (OutputTuple outTuple : outTuples)
							out.collect(outTuple);

					}
				}).setParallelism(1).name("aggregator");

		SingleOutputStreamOperator<Tuple1<String>> convertedOutput = outStream
				.flatMap(
						new RichFlatMapFunction<OutputTuple, Tuple1<String>>() {

							SimpleDateFormat sdf = new SimpleDateFormat(
									"yyyy-MM-dd HH:mm:ss");
							NumberFormat formatter = new DecimalFormat("#0.000");

							public void open(Configuration parameters)
									throws Exception {
								sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
							}

							@Override
							public void flatMap(OutputTuple t,
									Collector<Tuple1<String>> out)
									throws Exception {
								out.collect(new Tuple1<String>(
										sdf.format(new Date((long) t.getTS()))
												+ ","
												+ t.getKey()
												+ ","
												+ formatter.format(t
														.getAvgCongestion())
												+ ","
												+ t.getContributingTuples()));

							}
						}).setParallelism(1).name("formatter");

		convertedOutput
				.writeAsText(params.getRequired("outputFile"),
						WriteMode.OVERWRITE).setParallelism(1).name("logger");

		env.setParallelism(1);
		env.execute("daisy");

		System.out.println("completed in "
				+ ((System.currentTimeMillis() - start) / 1000) + " seconds");

	}
}
