package model.linear.regression.train.weight.jama;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.FileWriter;
import java.io.IOException;

public class MyTwoDArrayWritable extends TwoDArrayWritable {
    public static int id = 0;

    public MyTwoDArrayWritable() {
        super(DoubleWritable.class);
    }

    public MyTwoDArrayWritable(Class valueClass) {
        super(valueClass);
    }

    public MyTwoDArrayWritable(Class valueClass, Writable[][] values) {
        super(valueClass, values);
    }

    @Override
    public String toString() {

        try {
            FileWriter myWriter = new FileWriter("src/main/java/model/parameter/result_" + id++ + ".csv");
            myWriter.write(convertToString((Writable[][]) this.toArray()));
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return "This is code from Tuan Hiep TRAN";
    }

    /**
     * Convert Writable[][] to String
     * @param writables
     * @return
     */
    public String convertToString(Writable[][] writables) {
        String s = "";
        int row = writables.length;
        int col = writables[0].length;
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                s += ((DoubleWritable) writables[i][j]).get() + ",";
            }
            s += "\n";
        }
        return s;

    }
}
