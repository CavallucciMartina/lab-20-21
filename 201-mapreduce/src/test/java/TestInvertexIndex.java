import exercise4.Ex4InvertedIndex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;


class TestInvertedIndex {


    @Test
    public void mapperBreakesTheRecord() throws IOException {
        new MapDriver<Object, Text, Text, LongWritable>()
                .withMapper(new Ex4InvertedIndex.Ex4Mapper())
                .withInput(new LongWritable(0), new Text("ciao amici miei"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("ciao"), new LongWritable(0)),
                        new Pair<>(new Text("amici"), new LongWritable(0)),
                        new Pair<>(new Text("miei"), new LongWritable(0))
                ))
                .runTest();
    }
    @Test
    public void testAvgReducer() throws IOException {
        new ReduceDriver<Text, LongWritable, Text, Text>()
                .withReducer(new Ex4InvertedIndex.Ex4Reducer())
                .withInput(new Text("amico"), Arrays.asList(new LongWritable(5), new LongWritable(7)))
                .withOutput(new Text("amico"), new Text("[5, 7]"))
                .runTest();
    }

    @Test
    public void testAverageWordLenght() throws IOException {
        new MapReduceDriver<Object, Text, Text, LongWritable, Text, Text>()
                .withMapper(new Ex4InvertedIndex.Ex4Mapper())
                .withReducer(new Ex4InvertedIndex.Ex4Reducer())
                .withInput(new LongWritable(0), new Text("ciao amici ciao ciao ciao"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("amici"), new Text("[0]")),
                        new Pair<>(new Text("ciao"), new Text("[0]"))
                ))
                .runTest();
    }
}
