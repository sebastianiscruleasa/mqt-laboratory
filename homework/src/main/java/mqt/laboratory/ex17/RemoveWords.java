package mqt.laboratory.ex17;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RemoveWords {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("RemoveWords").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        String firstFilePath = "/Users/sebastian.iscruleasa/Facultate/Master I/MQT/mqt-laboratory/homework/src/main/java/mqt/laboratory/ex17/first_file.txt";
        JavaRDD<String> firstTextFile = sparkContext.textFile(firstFilePath);

        String secondFilePath = "/Users/sebastian.iscruleasa/Facultate/Master I/MQT/mqt-laboratory/homework/src/main/java/mqt/laboratory/ex17/second_file.txt";
        JavaRDD<String> secondTextFile = sparkContext.textFile(secondFilePath);

        JavaRDD<String> wordsToBeRemoved = secondTextFile
                .flatMap(line -> Arrays.asList(line.split("[\\s,;]")).iterator())
                .distinct();

        JavaRDD<String> filteredWords = firstTextFile
                .flatMap(line -> Arrays.asList(line.split("[\\s,;]")).iterator())
                .subtract(wordsToBeRemoved);

        String outputPath = "/Users/sebastian.iscruleasa/Facultate/Master I/MQT/mqt-laboratory/homework/src/main/java/mqt/laboratory/ex17/output.txt";
        filteredWords.coalesce(1).saveAsTextFile(outputPath);

        sparkContext.stop();
    }
}
