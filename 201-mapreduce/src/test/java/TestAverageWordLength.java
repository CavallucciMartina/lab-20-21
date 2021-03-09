import exercise4.Ex4AverageWordLength;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.Arrays;


class TestAvgWordLenght {
    @Test
    public void mapperBreakesTheRecord() throws IOException {
        new MapDriver<Object, Text, Text, IntWritable>()
                .withMapper(new Ex4AverageWordLength.Ex4Mapper())
                .withInput(new LongWritable(0), new Text("ciao amici miei"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("c"), new IntWritable(4)),
                        new Pair<>(new Text("a"), new IntWritable(5)),
                        new Pair<>(new Text("m"), new IntWritable(4))
                ))
                .runTest();
    }
    @Test
    public void testAvgReducer() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, DoubleWritable>()
                .withReducer(new Ex4AverageWordLength.Ex4Reducer())
                .withInput(new Text("a"), Arrays.asList(new IntWritable(5), new IntWritable(7)))
                .withOutput(new Text("a"), new DoubleWritable(6))
                .runTest();
    }


    @Test
    public void testAverageWordLenght() throws IOException {
        new MapReduceDriver<Object, Text, Text, IntWritable, Text, DoubleWritable>()
                .withMapper(new Ex4AverageWordLength.Ex4Mapper())
                .withReducer(new Ex4AverageWordLength.Ex4Reducer())
                .withInput(new LongWritable(0), new Text("ciaoo amici ciao ciaoo ciao"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("a"), new DoubleWritable(5)),
                        new Pair<>(new Text("c"), new DoubleWritable(4.5))
                ))
                .runTest();
    }
}
