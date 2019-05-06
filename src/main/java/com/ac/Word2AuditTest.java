package com.ac;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public final class Word2AuditTest {
    private static FileSystem getFileSystem(String ahdfs) throws Exception {
				Configuration configuration = new Configuration();
				configuration.set("fs.defaultFS", "hadoop.user");
				configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return FileSystem.get(new URI(ahdfs),configuration);
    }

    public static void main(String[] args) throws Exception {

        System.out.println("*** AuditJava JX BOSSvsCRM 1.2 ***");
        //System.out.println(System.getProperty("java.class.path"));

        if (args.length < 3) {
            System.err.println("Usage: Word2AuditTest <HDFS> <Pathin/> <Pathout/> [ParaNum] [SparkHome]");
            System.exit(1);
        }
        String aPath = args[0]+args[1];
        String aOutPath = args[0]+args[2];

        FileSystem fs = getFileSystem(args[0]);

        SparkSession.Builder sparkB = SparkSession
                .builder()
                .appName("AuditJava_JX_BOSSvsCRM_1.2");
        if (args.length >= 5) {
            sparkB.master(args[4]);
        }

        if (args.length >= 4) {
            sparkB.config("spark.default.parallelism", args[3]);
        }
        
        //SparkConf scf = new SparkConf();
        //scf.setJars(new String[]{"hdfs://shionlnx:9000/bin/Word2AuditTest-1.1.jar"});
        //scf.setJars(new String[]{System.getProperty("java.class.path")});        
        //sparkB.config(scf);

        SparkSession spark = sparkB.getOrCreate();

        JavaRDD<Row> dfboss = spark.read()
                .option("sep", "|")
                .option("encoding", "GBK")
                .csv(aPath + "zk.cm_customer*.txt")
                .javaRDD();
        JavaPairRDD<String, String[]> rddboss = dfboss
                .mapToPair(s ->
                        new Tuple2<>(s.getString(0), new String[]{s.getString(1), s.getString(2), s.getString(3)})
                )
                .reduceByKey((i1, i2) -> i1);

        JavaRDD<Row> dfhss = spark.read()
                .option("sep", "|")
                .option("encoding", "GBK")
                .csv(aPath + "zg.crm_customer*.txt")
                .javaRDD();
        JavaPairRDD<String, String[]> rddhss = dfhss
                .mapToPair(s ->
                        new Tuple2<>(s.getString(0), new String[]{s.getString(1), s.getString(2), s.getString(3)})
                )
                .reduceByKey((i1, i2) -> i1);


        JavaPairRDD<String, Tuple2<Optional<String[]>, Optional<String[]>>> out_b_s = rddboss.fullOuterJoin(rddhss);

        int g001 = 0;
        FSDataOutputStream oFf001 = fs.create(new Path(String.format("%sC_CUSTOMER_001_J.TXT", aOutPath)));
        OutputStreamWriter oF001 = new OutputStreamWriter(oFf001);
        List<Tuple2<String, Tuple2<Optional<String[]>, Optional<String[]>>>> output1 = out_b_s.filter(s -> !s._2()._2().isPresent()).collect();
        for (Tuple2<String, Tuple2<Optional<String[]>, Optional<String[]>>> tuple : output1) {
            if (tuple._1().length() != 0) {
                oF001.write(String.format("%s|%s|%s|%s\n", tuple._1(), tuple._2()._1().get()[0], tuple._2()._1().get()[1], tuple._2()._1().get()[2]));
                g001++;
            }
        }
        oF001.close();

        int g002 = 0;
        FSDataOutputStream oFf002 = fs.create(new Path(String.format("%sC_CUSTOMER_002_J.TXT", aOutPath)));
        OutputStreamWriter oF002 = new OutputStreamWriter(oFf002);
        List<Tuple2<String, Tuple2<Optional<String[]>, Optional<String[]>>>> output2 = out_b_s.filter(s -> !s._2()._1().isPresent()).collect();
        for (Tuple2<String, Tuple2<Optional<String[]>, Optional<String[]>>> tuple : output2) {
            if (tuple._1().length() != 0) {
                oF002.write(String.format("%s|%s|%s|%s\n", tuple._1(), tuple._2()._2().get()[0], tuple._2()._2().get()[1], tuple._2()._2().get()[2]));
                g002++;
            }
        }
        oF002.close();

        int g0xx = 0;

        int g003 = 0;
        int g004 = 0;
        int g005 = 0;
        FSDataOutputStream oFf003 = fs.create(new Path(String.format("%sC_CUSTOMER_003_J.TXT", aOutPath)));
        OutputStreamWriter oF003 = new OutputStreamWriter(oFf003);
        FSDataOutputStream oFf004 = fs.create(new Path(String.format("%sC_CUSTOMER_004_J.TXT", aOutPath)));
        OutputStreamWriter oF004 = new OutputStreamWriter(oFf004);
        FSDataOutputStream oFf005 = fs.create(new Path(String.format("%sC_CUSTOMER_005_J.TXT", aOutPath)));
        OutputStreamWriter oF005 = new OutputStreamWriter(oFf005);
        List<Tuple2<String, Tuple2<Optional<String[]>, Optional<String[]>>>> output3 = out_b_s.filter(s
                -> s._2()._1().isPresent()
                && s._2()._2().isPresent()
                && !Arrays.equals(s._2()._1().get(), s._2()._2().get())
        ).collect();
        for (Tuple2<String, Tuple2<Optional<String[]>, Optional<String[]>>> tuple : output3) {
            if (!StringUtils.equals(tuple._2()._1().get()[0], tuple._2()._2().get()[0])) {
                oF003.write(String.format("%s|%s|%s\n", tuple._1(), tuple._2()._1().get()[0], tuple._2()._2().get()[0]));
                g003++;
            }
            if (!StringUtils.equals(tuple._2()._1().get()[1], tuple._2()._2().get()[1])) {
                oF004.write(String.format("%s|%s|%s\n", tuple._1(), tuple._2()._1().get()[1], tuple._2()._2().get()[1]));
                g004++;
            }
            if (!StringUtils.equals(tuple._2()._1().get()[2], tuple._2()._2().get()[2])) {
                oF005.write(String.format("%s|%s|%s\n", tuple._1(), tuple._2()._1().get()[2], tuple._2()._2().get()[2]));
                g005++;
            }
            g0xx++;
        }
        oF003.close();
        oF004.close();
        oF005.close();

        System.out.printf("001: %d\n", g001);
        System.out.printf("002: %d\n", g002);
        System.out.printf("存在并不一致: %d\n", g0xx);
        System.out.printf("003: %d\n", g003);
        System.out.printf("004: %d\n", g004);
        System.out.printf("005: %d\n", g005);

        FSDataOutputStream fflog = fs.create(new Path(String.format("%sCvB_SparkJavaLog.log", aOutPath)));
        OutputStreamWriter flog = new OutputStreamWriter(fflog);
        flog.write(String.format("001: %d\n", g001));
        flog.write(String.format("002: %d\n", g002));
        flog.write(String.format("存在并不一致: %d\n", g0xx));
        flog.write(String.format("003: %d\n", g003));
        flog.write(String.format("004: %d\n", g004));
        flog.write(String.format("005: %d\n", g005));
        flog.close();
        spark.stop();

        System.out.println("*** Task done ***");
    }
}
