package model.linear.regression.compute.cost;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class LRComputeCostReducer extends MapReduceBase implements Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {

    @Override
    public void reduce(LongWritable key, Iterator<FloatWritable> value,
                       OutputCollector<LongWritable, FloatWritable> output, Reporter reporter)
            throws IOException {

        float sum = 0;
        // sum all the values of a given key
        while (value.hasNext()) {
            sum += value.next().get();
        }
        output.collect(key, new FloatWritable(sum));
    }

}
