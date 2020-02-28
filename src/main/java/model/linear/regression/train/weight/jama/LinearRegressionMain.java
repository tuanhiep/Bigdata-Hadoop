package model.linear.regression.train.weight.jama;

import Jama.Matrix;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.Thread.sleep;

public class LinearRegressionMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        // Delete folder output if it exists
        File index = new File("src/main/resources/output");
        if (index.exists()) {
            String[] entries = index.list();
            for (String s : entries) {
                File currentFile = new File(index.getPath(), s);
                currentFile.delete();
            }
            index.delete();
        }
        // Start configure the job MapReduce
        JobConf conf = new JobConf(LinearRegressionMain.class);
        conf.setJobName("linearRegression");
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(MyTwoDArrayWritable.class);
        conf.setMapperClass(LinearRegressionMapper.class);
        conf.setCombinerClass(LinearRegressionReducer.class);
        conf.setReducerClass(LinearRegressionReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        sleep(3000);
        // Compute weights for linear regression model
        LinearRegressionClassifier classifier = new LinearRegressionClassifier();
        Matrix XtX = classifier.readMatrixFromFile("src/main/java/model/parameter/result_0.csv");
        Matrix Xty = classifier.readMatrixFromFile("src/main/java/model/parameter/result_1.csv");
        // Calculate the weights of linear regression model by normal equation
        Matrix weights = (XtX.inverse()).times(Xty);
        classifier.writeMatrixToFile(weights, "src/main/java/model/parameter/weights.csv");


    }
}
