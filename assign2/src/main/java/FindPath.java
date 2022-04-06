import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class FindPath {
    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
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

    static class Node {
        private double id;
        private double lat;
        private double lon;

        public Node(double id, double lat, double lon) {
            this.id = id;
            this.lat = lat;
            this.lon = lon;
        }

        public double getId() {
            return this.id;
        }

        public double getLat() {
            return this.lat;
        }
        
        public double getLon() {
            return this.lon;
        }
    }
    
    private class Road {
        private UUID id;
        private double src;
        private double dst;

        public Road(double src, double dst) {
            this.id = UUID.randomUUID();
            this.src = src;
            this.dst = dst;
        }

        public UUID getId() {
            return this.id;
        }
        
        public double getSrc() {
            return this.src;
        }
        
        public double getDst() {
            return this.dst;
        }
    }

    static MapFunction<Row,Node> mapToNode = (Row row) -> {
        double id = row.getAs("_id");
        double lat = row.getAs("_lat");
        double lon = row.getAs("_lon");
        return new Node(id, lat, lon);
    }; 

    public static void main(String[] args) {
        

        SparkSession spark = SparkSession
            .builder()
            .appName("BuildMap Application")
            .getOrCreate();
        Dataset<Row> nodeData = spark.read().format("xml").option("rowTag", "node").load(args[0]);
        Dataset<Row> roadData = spark.read().format("xml").option("rowTag", "way").load(args[0]);
        roadData.select("_id").show();
        List<Node> nodes = nodeData.map(mapToNode, Encoders.bean(Node.class)).collectAsList();
        for (Node n : nodes) {
            System.out.println(n.getId());
            System.out.println(n.getLat());
            System.out.println(n.getLon());
            System.out.println();
        }
        spark.stop();
    }
}
