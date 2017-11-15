package io.pendul.mrnf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;

import static org.apache.hadoop.fs.FileSystem.get;

/**
 * This is the main class for the MapReduce for Integer factorization, using the quadratic sieve algorithm.
 */
public class QuadricSieveDriver extends Configured implements Tool {

    // Logger for the MapReduce class.
    private static final Logger LOGGER = LoggerFactory.getLogger(QuadricSieveDriver.class);

    /**
     * Each map input will be a sieve interval of the given size.
     * <p>
     * It is currently set to 10 for testing. Change to 1000000 for production
     * use. 1 million BigIntegers in ASCII ~ 12 - 15 MB.
     */
    public static final int MAP_INPUT_INTERVAL_SIZE = 10;

    /**
     * The name of the input file for the mapreduce.
     */
    public static final String INPUT_FILE_NAME = "/sieve-input-" + System.currentTimeMillis();

    /**
     * Name of the MapRedce variable that holds the integer to factor.
     */
    public static final String INTEGER_TO_FACTOR_NAME = "N";

    /**
     * Name of the MapReduce variable that holds the factor base.
     */
    public static final String FACTOR_BASE_NAME = "FactorBase";

    private Path inputPath;
    private Path outputPath;

    /**
     * Setup the MapReduce parameters and run it.
     * <p>
     * Tool parses the command line arguments for us.
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // Check the arguments. we need the integer to attempt to factor.
        if (args.length < 1) {
            System.out.println("Please indicate the integer to factor");
            LOGGER.error("No integer to factor. Exit.");
            System.exit(1);
        }

        // Parse N and add it to the job configuration, so that the workers can
        // access it as well.
        BigInteger N = new BigInteger(args[0]);
        LOGGER.info("Attempting factorization of: {}", N);
        conf.set(INTEGER_TO_FACTOR_NAME, N.toString());

        // Obtain the factor base for the integer N.
        FactorBaseArray factorBase = SieveInput.factorBase(N);
        LOGGER.info("Factor base of size: {}", factorBase.size());
        conf.set(FACTOR_BASE_NAME, factorBase.toString());

        try (FileSystem fs = get(conf)) {
            // set up input path
            inputPath = fs.getHomeDirectory().suffix("/input");
            if (!fs.exists(inputPath) || !fs.isDirectory(inputPath)) fs.mkdirs(inputPath);
            LOGGER.info("Input path: {}", inputPath);

            // set up output path
            outputPath = fs.getHomeDirectory().suffix("/output-" + System.currentTimeMillis());
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            // Prepare the input of the mapreduce.
            LOGGER.info("Sieve of size: " + SieveInput.fullSieveIntervalSize(N));
            try {
                // Write the full sieve interval to disk.
                SieveInput.writeFullSieveInterval(N, fs, inputPath.suffix(INPUT_FILE_NAME));
            } catch (FileNotFoundException e) {
                System.out.println("Unable to open the file for writing.");
            } catch (IOException e) {
                System.out.println("Unable to write to the output file.");
            }
        }

        // Configure the classes of the mapreducer
        Job job = new Job(conf, "QuadraticSieve");
        job.setJarByClass(QuadricSieveDriver.class);
        job.setMapperClass(SieveMapper.class);
        job.setReducerClass(FindSquaresReducer.class);

        // Output will be two pairs of strings:
        // <"Factor1", "59">
        // <"Factor2", "101">
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Submit the job.
        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Run the MapReduce.
     */
    public static void main(String[] args) throws Exception {
        // ToolRunner will parse common command line options
        int result = ToolRunner.run(new Configuration(), new QuadricSieveDriver(), args);
        System.exit(result);
    }
}
