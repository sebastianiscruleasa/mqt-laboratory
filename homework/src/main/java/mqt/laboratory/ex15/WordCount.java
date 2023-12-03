package mqt.laboratory.ex15;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        String filePath = "/Users/sebastian.iscruleasa/Facultate/Master I/MQT/mqt-laboratory/homework/src/main/java/mqt/laboratory/ex15/randomText.txt";
        JavaRDD<String> textFile = sparkContext.textFile(filePath);

        long count = textFile
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(word -> word.equals("master"))
                .count();

        System.out.println("Occurrences of 'master': " + count);
        sparkContext.stop();
    }
}
