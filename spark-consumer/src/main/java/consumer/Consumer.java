package consumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import dto.Forecasting;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.*;
import scala.reflect.ClassTag;

import java.lang.Byte;
import java.lang.Double;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.util.StringHelper.join;

public class Consumer {
    private String kafkaBootstrapServers = "localhost:9092";
    private String topicName = "test";
    private Map<String, Object> kafkaParams = new HashMap<>();
    private Collection<String> topics;
    private String pathToSourceFile;
    private String checkpointDir = "D:\\checkpoint";

    public Consumer() {
        kafkaParams.put("bootstrap.servers", kafkaBootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        topics = Arrays.asList(topicName);
    }

    public void consumeMessages() throws InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\kafka");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(5000));
        JavaInputDStream<ConsumerRecord<String, byte[]>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams)
                );
        streamingContext.checkpoint(checkpointDir);
        JavaDStream<Forecasting> cachedForecating = stream.map(x -> x.value())
                .map(record -> {
                    GenericRecord genericRecord = GenericAvroCodecs.toBinary(ReflectData.get().getSchema(Forecasting.class)).invert(record).get();
                    Forecasting forecasting = Forecasting.builder()
                            .NOC(genericRecord.get("NOC").toString())
                            .description(genericRecord.get("description").toString())
                            .industry(genericRecord.get("industry").toString())
                            .variable(genericRecord.get("variable").toString())
                            .geografickArea(genericRecord.get("geografickArea").toString())
                            .year2018(Integer.parseInt(genericRecord.get("year2018").toString()))
                            .year2019(Integer.parseInt(genericRecord.get("year2019").toString()))
                            .year2020(Integer.parseInt(genericRecord.get("year2020").toString()))
                            .year2021(Integer.parseInt(genericRecord.get("year2021").toString()))
                            .year2022(Integer.parseInt(genericRecord.get("year2022").toString()))
                            .year2023(Integer.parseInt(genericRecord.get("year2023").toString()))
                            .year2024(Integer.parseInt(genericRecord.get("year2024").toString()))
                            .year2025(Integer.parseInt(genericRecord.get("year2025").toString()))
                            .year2026(Integer.parseInt(genericRecord.get("year2026").toString()))
                            .year2027(Integer.parseInt(genericRecord.get("year2027").toString()))
                            .year2028(Integer.parseInt(genericRecord.get("year2028").toString()))
                            .fistFiveYearPercentage(Double.parseDouble(genericRecord.get("fistFiveYearPercentage").toString()))
                            .secondFiveYearPercentage(Double.parseDouble(genericRecord.get("secondFiveYearPercentage").toString()))
                            .decadePercentage(Double.parseDouble(genericRecord.get("decadePercentage").toString()))
                            .build();

                    return forecasting;
                })
                .cache();

        Function3<String, Optional<Double>, State<Double>, Tuple2<String, Double>> mappingFunction =
                (key, value, state) -> {
                    double average = (value.get() + (state.exists() ? state.get() : 0)) / 2;
                    Tuple2<String, Double> tuple2 = new Tuple2<>(key, average);
                    if(state.exists()){
                        System.out.println(key+"Yeeesssss"+state.get());
                    }
                    state.update(average);
                    return tuple2;
                };

        JavaPairDStream<String, Double> secondFiveYearPercentageAverageByIndustry = cachedForecating.mapToPair(forecasting -> new Tuple2<>(forecasting.getIndustry(), forecasting.getSecondFiveYearPercentage()))
                .groupByKey()
                .mapToPair(tuple2 -> {
                    List<Double> doubleList = new ArrayList<>();
                    tuple2._2.forEach(x->doubleList.add(x));
                    return new Tuple2<>(tuple2._1, doubleList.stream().mapToDouble(x->x).sum()/doubleList.size());
                })
                .mapWithState(StateSpec.function(mappingFunction))
                .stateSnapshots()
                .mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2));

        cachedForecating.mapToPair(forecasting -> new Tuple2<>(forecasting.getIndustry(), forecasting.getFistFiveYearPercentage()))
                .groupByKey()
                .mapToPair(tuple2 -> {
                    List<Double> doubleList = new ArrayList<>();
                    tuple2._2.forEach(x->doubleList.add(x));
                    return new Tuple2<>(tuple2._1, doubleList.stream().mapToDouble(x->x).sum()/doubleList.size());
                })
                .mapWithState(StateSpec.function(mappingFunction))
                .stateSnapshots()
                .mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2))
                .join(secondFiveYearPercentageAverageByIndustry)
                .mapValues(values -> {
                    System.out.println(values._1 + ", " + values._2());
                    return values._1 + ", " + values._2();
                })
                .foreachRDD(rdd -> rdd.saveAsTextFile("D:\\result"));

//        JavaDStream<Tuple2<String, Double>> maxForecastingByIndustry = cachedForecating.mapToPair(forecasting -> new Tuple2<>(forecasting.getIndustry(), new ArrayList<Integer>(Arrays.asList(forecasting.getYear2018(), forecasting.getYear2019(), forecasting.getYear2020(), forecasting.getYear2021(), forecasting.getYear2022(), forecasting.getYear2023(), forecasting.getYear2024(), forecasting.getYear2025(), forecasting.getYear2026(), forecasting.getYear2027(), forecasting.getYear2028()))))
//                .groupByKey()
//                .map(tuple2 -> {
//                    int max = tuple2._2.iterator().next().stream().max(Integer::compareTo).get();
//                    while (tuple2._2.iterator().hasNext()) {
//                        int tempMax = tuple2._2.iterator().next().stream().max(Integer::compareTo).get();
//                        if (max < tempMax) {
//                            max = tempMax;
//                        }
//                    }
//                    return new Tuple2(tuple2._1, max);
//                });
//
//
//        cachedForecating.map(forecasting -> new Tuple4<>(forecasting.getGeografickArea(), forecasting.getIndustry(), forecasting.getFistFiveYearPercentage(), forecasting.getSecondFiveYearPercentage()))
//                .filter(tuple4 -> tuple4._4() > tuple4._3())
//                .foreachRDD(rdd -> {
//                    rdd.sortBy(tuple4 -> tuple4._1(), false, 1)
//                            .groupBy(tuple4 -> tuple4._1())
//                            .mapValues(values -> {
//                                List<Tuple4<String, String, Double, Double>> tuple4List = new ArrayList<>();
//                                while (values.iterator().hasNext()) {
//                                    tuple4List.add(values.iterator().next());
//                                }
//                                tuple4List.sort(Comparator.comparing(x -> x._4() - x._3()));
//                                tuple4List = tuple4List.stream()
//                                        .limit(10)
//                                        .collect(Collectors.toList());
//                                return tuple4List;
//                            })
//                            .saveAsTextFile("D:\\myResult.txt");
//                });


        //показать по районам индустрии отсортированных по убыванию роста инлдустрии
        streamingContext.start();
        streamingContext.awaitTermination();

    }


}
