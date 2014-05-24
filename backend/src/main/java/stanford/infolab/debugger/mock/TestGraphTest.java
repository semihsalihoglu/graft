package stanford.infolab.debugger.mock;

import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class TestGraphTest {

  public static void main(String[] args) {
    TestGraph<LongWritable, DoubleWritable, NullWritable> graph =
        new TestGraph<LongWritable, DoubleWritable, NullWritable>(new GiraphConfiguration());

    Entry<LongWritable, NullWritable>[] edges0 = new Entry[3];

    edges0[0] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(4l), NullWritable.get());
    edges0[1] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(2l), NullWritable.get());
    edges0[2] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(3l), NullWritable.get());

    graph.addVertex(new LongWritable(1l), new DoubleWritable(0.0d), edges0);

    Entry<LongWritable, NullWritable>[] edges1 = new Entry[2];

    edges1[0] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(3l), NullWritable.get());
    edges1[1] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(2l), NullWritable.get());

    graph.addVertex(new LongWritable(4l), new DoubleWritable(0.0d), edges1);

    Entry<LongWritable, NullWritable>[] edges2 = new Entry[1];

    edges2[0] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(1l), NullWritable.get());

    graph.addVertex(new LongWritable(2l), new DoubleWritable(0.0d), edges2);

    Entry<LongWritable, NullWritable>[] edges3 = new Entry[0];


    graph.addVertex(new LongWritable(3l), new DoubleWritable(0.0d), edges3);

    Entry<LongWritable, NullWritable>[] edges4 = new Entry[2];

    edges4[0] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(2l), NullWritable.get());
    edges4[1] =
        new SimpleEntry<LongWritable, NullWritable>(new LongWritable(4l), NullWritable.get());

    graph.addVertex(new LongWritable(5l), new DoubleWritable(0.0d), edges4);

  }
}
