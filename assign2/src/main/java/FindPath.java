import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
        private long id;
        private double lat;
        private double lon;

        public Node() {};

        public Node(long id, double lat, double lon) {
            this.id = id;
            this.lat = lat;
            this.lon = lon;
        }

        public long getId() {
            return this.id;
        }

        public double getLat() {
            return this.lat;
        }

        public double getLon() {
            return this.lon;
        }

        public void setId(long id) {
            this.id = id;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public void setLon(double lon) {
            this.lon = lon;
        }
    }

    public static class Road implements Serializable {
        private UUID id;
        private long src;
        private long dst;

        public Road() {
            this.id = UUID.randomUUID();
        };

        public Road(long src, long dst) {
            this.id = UUID.randomUUID();
            this.src = src;
            this.dst = dst;
        }

        public UUID getId() {
            return this.id;
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
            n.setId(row.getAs("_id"));
            n.setLat(row.getAs("_lat"));
            n.setLon(row.getAs("_lon"));
            return n;
        }
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("BuildMap Application")
                .getOrCreate();
        Dataset<Row> nodeData = spark.read().format("xml").option("rowTag", "node").load(args[0]);
        // Dataset<Row> roadData = spark.read().format("xml").option("rowTag", "way").load(args[0]).filter();
        for (int i = 0; i < nodeData.dtypes().length; i++) {
            System.out.println(nodeData.dtypes()[i]);
        }
        List<Node> nodes = nodeData.map(new NodeMapper(), Encoders.bean(Node.class)).collectAsList();
        // List<Road> roads = roadData.map(new RoadMapper(), Encoders.bean(Road.class)).collectAsList();
        spark.stop();
    }
}
