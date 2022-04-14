import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.Collection;
import java.util.UUID;

public class FindPath {
    // From:
    // https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    public static class Node implements Serializable {
        private long nid;
        private double lat;
        private double lon;

        public Node() {
        };

        public Node(long nid, double lat, double lon) {
            this.nid = nid;
            this.lat = lat;
            this.lon = lon;
        }

        public long getNid() {
            return this.nid;
        }

        public double getLat() {
            return this.lat;
        }

        public double getLon() {
            return this.lon;
        }

        public void setNid(long nid) {
            this.nid = nid;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public void setLon(double lon) {
            this.lon = lon;
        }
    }

    public static class Road implements Serializable {
        private UUID rid;
        private long src;
        private long dst;

        public Road() {
            this.rid = UUID.randomUUID();
        };

        public Road(long src, long dst) {
            this.rid = UUID.randomUUID();
            this.src = src;
            this.dst = dst;
        }

        public UUID getRid() {
            return this.rid;
        }

        public long getSrc() {
            return this.src;
        }

        public long getDst() {
            return this.dst;
        }

        public void setSrc(long src) {
            this.src = src;
        }

        public void setDst(long dst) {
            this.dst = dst;
        }
    }

    public static class NodeMapper implements MapFunction<Row, Node> {
        @Override
        public Node call(Row row) throws Exception {
            Node n = new Node();
            n.setNid(row.getAs("_id"));
            n.setLat(row.getAs("_lat"));
            n.setLon(row.getAs("_lon"));
            return n;
        }
    };

    public static class RoadMapper implements FlatMapFunction<Row, Road> {
        @Override
        public Iterator<Road> call(Row row) throws Exception {
            List<Road> roads = new ArrayList<>();
            List<Row> nodes = row.getList(7);
            List<Row> tags = row.getList(8);
            Boolean isHighway = Stream.ofNullable(tags).flatMap(Collection::stream).anyMatch(
                    tag -> tag.getAs("_k").toString().equals("highway"));
            Boolean isOneway = Stream.ofNullable(tags).flatMap(Collection::stream).anyMatch(
                    tag -> tag.getAs("_k").toString().equals("oneway") && tag.getAs("_v").toString().equals("yes"));
            if (isHighway) {
                for (int i = 0; i < nodes.size() - 1; i++) {
                    long src = nodes.get(i).getAs("_ref");
                    long dst = nodes.get(i + 1).getAs("_ref");
                    roads.add(new Road(src, dst));
                    if (!isOneway) {
                        roads.add(new Road(dst, src));
                    }
                }
            }
            return roads.iterator();
        }
    };

    public static class NeighborMapper implements ForeachFunction<Row> {
        private FSDataOutputStream dos;

        NeighborMapper(FSDataOutputStream dos) {
            this.dos = dos;
        }
        @Override
        public void call(Row row) throws Exception {
            dos.writeBytes(row.toString());
        }
    };

    // public static void writeToFile(Dataset<Row> ds, String outPath) throws IOException {
    //     Configuration config = new Configuration();
    //     FileSystem fs = FileSystem.get(config);
    //     FSDataOutputStream dos = fs.create(new Path(outPath));
    //     dos.writeBytes("hello world");
    //     ds.foreach((Row r) -> dos.writeBytes(r.getAs("nid").toString() + "\n"));
    //     // + gf.triplets().filter(gf.col("src").id ==
    //     // r.getAs("nid")).select("dst").collectAsList().toString()));
    // }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("FindPathApplication")
                .getOrCreate();
        Dataset<Row> nodeData = spark.read().format("xml").option("rowTag", "node").load(args[0]);
        Dataset<Row> roadData = spark.read().format("xml").option("rowTag", "way").load(args[0]);
        for (int i = 0; i < nodeData.dtypes().length; i++) {
            System.out.println(nodeData.dtypes()[i]);
        }
        System.out.println();
        for (int i = 0; i < roadData.dtypes().length; i++) {
            System.out.println(roadData.dtypes()[i]);
        }
        List<Node> nodes = nodeData.map(new NodeMapper(), Encoders.bean(Node.class)).collectAsList();
        List<Road> roads = roadData.flatMap(new RoadMapper(), Encoders.bean(Road.class)).collectAsList();
        Dataset<Row> vertices = spark.createDataFrame(nodes, Node.class);
        Dataset<Row> edges = spark.createDataFrame(roads, Road.class);
        GraphFrame graph = new GraphFrame(vertices, edges);
        graph.vertices().show();
        graph.edges().show();
        Dataset<Row> joined = vertices.join(edges, vertices.col("nid").equalTo(edges.col("src")), "left_outer");
        joined.show();
        Dataset<Row> collected = joined.groupBy("nid").agg(functions.collect_set("dst").as("dsts"));
        collected.show();
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            FSDataOutputStream dos = fs.create(new Path(args[2]));
            collected = collected.withColumn("dsts", functions.concat_ws(" ", collected.col("dsts")));
            collected = collected.select(functions.concat_ws(" ", collected.col("nid"), collected.col("dsts")));
            List<String> result = collected.map((MapFunction<Row, String>)row -> row.mkString(), Encoders.STRING()).collectAsList();
            for (String r : result) {
                dos.writeBytes(r + "\n");
            }
            // collected.foreach((ForeachFunction<Row>) r -> dos.writeBytes(r.mkString()));
            // dos.writeBytes(collected.collect().toString());
            // collected.foreach((ForeachFunction<Row>) r -> dos.writeBytes(r.getAs("nid").toString() + "\n")
            // + gf.triplets().filter(gf.col("src").id ==
            // r.getAs("nid")).select("dst").collectAsList().toString());
        } catch (Exception e) {
            System.err.println(e);
        }
        // collected.select("nid").coalesce(1).write().text(args[2]);
        spark.stop();
    }
}
