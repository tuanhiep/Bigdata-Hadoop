package model.linear.regression.train.weight.jama;

import Jama.Matrix;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LinearRegressionClassifier {
    /**
     * Read Matrix Jama from file
     *
     * @param path
     * @return
     * @throws IOException
     */
    public Matrix readMatrixFromFile(String path) throws IOException {
        File file = new File(path);

        BufferedReader br = new BufferedReader(new FileReader(file));
        ArrayList<double[]> matrix = new ArrayList<>();
        String line;
        int col = 1;
        while ((line = br.readLine()) != null) {
            String[] features = line.split(",");
            col = features.length;
            // matrix Xi for 1 row = 1 sample of data set
            double[] xi = new double[col];
            for (int j = 0; j < col; j++) {
                xi[j] = Double.valueOf(features[j]);
            }
            matrix.add(xi);
        }
        int row = matrix.size();
        double[][] x = new double[row][col];
        for (int i = 0; i < row; i++) {
            x[i] = matrix.get(i);
        }
        Matrix X = new Matrix(x);
        return X;
    }

    /**
     * Write Matrix Jama to file
     *
     * @param matrix
     * @param path
     */
    public void writeMatrixToFile(Matrix matrix, String path) {
        int row = matrix.getRowDimension();
        int col = matrix.getColumnDimension();
        String s = "";
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                if (j == col - 1) {
                    s += matrix.get(i, j);

                } else {
                    s += matrix.get(i, j) + ",";
                }
            }
            s += "\n";
        }

        try {
            FileWriter myWriter = new FileWriter(path);
            myWriter.write(s);
            myWriter.close();
            System.out.println("Successfully wrote weights to the file at " + path);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

}


