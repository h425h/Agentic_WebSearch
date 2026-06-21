package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import java.util.Arrays;

public class TestJob {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        FlameRDD rdd = ctx.parallelize(Arrays.asList("hello world", "foo bar", "hello foo"));
        int count = rdd.count();
        FlameRDD words = rdd.flatMap(s -> Arrays.asList(s.split(" ")));
        int wordCount = words.count();
        ctx.output("OK - " + count + " strings, " + wordCount + " words");
    }
}
