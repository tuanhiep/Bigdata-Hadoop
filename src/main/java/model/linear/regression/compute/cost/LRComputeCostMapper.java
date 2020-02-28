package model.linear.regression.compute.cost;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LRComputeCostMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, FloatWritable> {
    private Path[] localFiles;
    FileInputStream fis = null;
    BufferedInputStream bis = null;

    @Override
    public void configure(JobConf job) {
        try {
            localFiles = DistributedCache.getLocalCacheFiles(job);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, FloatWritable> output,
                    Reporter reporter) throws IOException {

        /**
         *
         * Linear-Regression costs function
         *
         * This will simply sum over the subset and calculate the predicted value
         * y_predict(x) for the given features values and the current theta values
         * Then it will subtract the true y values from the y_predict(x) value for
         * every input record in the subset
         *
         * J(theta) = sum((y_predict(x)-y)^2)
         * y_predict(x) = theta(0)*x(0) + .... + theta(i)*x(i)
         *
         */
        // value represents one sample of dataset
        String line = value.toString();
        String[] features = line.split(",");
        List values = new ArrayList();
        for (int i = 0; i < features.length; i++) {
            values.add(new Float(features[i]));
        }
        output.collect(new LongWritable(1), new FloatWritable(costs(values)));
    }

    private final float costs(List values) {
//      read the weights parameters from file
        File file = new File(localFiles[0].toString());
        float costs = 0;
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            BufferedReader d = new BufferedReader(new InputStreamReader(bis));
            String line = d.readLine();
            String[] theta = line.split(",");
            // get the label y
            float y = (float) values.get(0);
/**
 * Calculate the costs for each record in values
 */
            for (int j = 0; j < values.size(); j++) {
//bias calculation
                if (j == 0)
                    costs += (new Float(theta[j])) * 1;
                else
                    costs += (new Float(theta[j])) * (float) values.get(j);
            }
// Subtract y and square the costs
            costs = (costs - y) * (costs - y);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return costs;

    }
}
