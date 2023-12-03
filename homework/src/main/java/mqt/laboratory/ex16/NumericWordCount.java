package mqt.laboratory.ex16;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class NumericWordCount {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("NumericValueCount").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        String filePath = "/Users/sebastian.iscruleasa/Facultate/Master I/MQT/mqt-laboratory/homework/src/main/java/mqt/laboratory/ex16/randomTextWithNumbers.txt";
        JavaRDD<String> textFile = sparkContext.textFile(filePath);

        textFile
                .flatMap(line -> Arrays.asList(line.split("[\\s,;]+|(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)")).iterator())
                .filter(NumericWordCount::isNumeric)
                .foreach(value -> System.out.println("Numbers: " + value));
        sparkContext.stop();
    }

    private static boolean isNumeric(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
