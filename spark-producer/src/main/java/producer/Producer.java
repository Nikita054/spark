package producer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import dto.Entrant;
import dto.Forecasting;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Producer implements Serializable {
    private String kafkaBootstrapServers = "localhost:9092";
    private String topicName = "test";
    private Map<String, Object> kafkaParams = new HashMap<>();
    private Collection<String> topics;
    private String pathToSourceFile;
    private Schema schema;
    private int RECORD_LENGTH = 19;

    public Producer() {
        kafkaParams.put("bootstrap.servers", kafkaBootstrapServers);
        kafkaParams.put("key.serializer", StringSerializer.class);
        kafkaParams.put("value.serializer", ByteArraySerializer.class);
        kafkaParams.put("group.id", "group2");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        topics = Arrays.asList(topicName);

    }

    public void produceMessages() {
        System.setProperty("hadoop.home.dir", "D:\\kafka");
//        consumer.subscribe(Arrays.asList("test"),new MyConsumerRebalancerListener(consumer));
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext ssc = new JavaSparkContext(sparkConf);
        Broadcast<Integer> br = ssc.broadcast(RECORD_LENGTH);
//        List<Entrant> entrants=ssc.textFile("D:\\sourceFile\\test.csv")
        ssc.textFile("D:\\sourceFile\\dataset.csv")
                .filter(record -> !record.startsWith("NOC"))
                .filter(record -> record.contains("%"))
                .map(record -> {
                    Forecasting forecasting = null;
                    if (record.contains("\"")) {
                        String[] splitedRecordOnQuote = record.split("\"");
                        List<String> splitedRecord = new ArrayList<>(Arrays.asList(splitedRecordOnQuote[0].split(",")));
                        for (int i = 1; i < splitedRecordOnQuote.length - 1; i += 2) {
                            if ((splitedRecordOnQuote[i].matches("^[a-zA-Z]+,*.*$")) && (i + 1 != splitedRecordOnQuote.length - 1) && (splitedRecordOnQuote[i + 1].matches("^,+[a-zA-Z ,]+$"))) {
                                splitedRecord.add(splitedRecordOnQuote[i]);
                                splitedRecord.addAll(Arrays.asList(splitedRecordOnQuote[i + 1].substring(1, splitedRecordOnQuote[i + 1].length() - 1).split(",")));
                            } else if ((splitedRecordOnQuote[i].matches("^[a-zA-Z]+,*.*$")) && (i + 1 != splitedRecordOnQuote.length - 1) && (splitedRecordOnQuote[i + 1].matches("^,*[a-zA-Z ,]+[0-9, .]+$"))) {
                                splitedRecord.add(splitedRecordOnQuote[i]);
                                splitedRecord.addAll(Arrays.asList(splitedRecordOnQuote[i + 1].substring(1, splitedRecordOnQuote[i + 1].length() - 1).replace(".", "").split(",")));
                            } else if (splitedRecordOnQuote[i].matches("^[a-zA-Z]+,*.*$")) {
                                splitedRecord.add(splitedRecordOnQuote[i].trim());
                            } else {
                                splitedRecord.add(splitedRecordOnQuote[i].replaceAll(",", ""));
                            }
                        }
                        splitedRecord.addAll(Arrays.asList(splitedRecordOnQuote[splitedRecordOnQuote.length - 1].substring(1).split(",")));
                        splitedRecord = splitedRecord.stream()
                                .filter(element -> element.length() > 0)
                                .map(element -> element.trim())
                                .collect(Collectors.toList());
                        if (splitedRecord.size() == br.getValue()) {
                            forecasting = Forecasting.builder()
                                    .NOC(splitedRecord.get(0))
                                    .description(splitedRecord.get(1))
                                    .industry(splitedRecord.get(2))
                                    .variable(splitedRecord.get(3))
                                    .geografickArea(splitedRecord.get(4))
                                    .year2018(Integer.parseInt(splitedRecord.get(5).replace(".", "")))
                                    .year2019(Integer.parseInt(splitedRecord.get(6).replace(".", "")))
                                    .year2020(Integer.parseInt(splitedRecord.get(7).replace(".", "")))
                                    .year2021(Integer.parseInt(splitedRecord.get(8).replace(".", "")))
                                    .year2022(Integer.parseInt(splitedRecord.get(9).replace(".", "")))
                                    .year2023(Integer.parseInt(splitedRecord.get(10).replace(".", "")))
                                    .year2024(Integer.parseInt(splitedRecord.get(11).replace(".", "")))
                                    .year2025(Integer.parseInt(splitedRecord.get(12).replace(".", "")))
                                    .year2026(Integer.parseInt(splitedRecord.get(13).replace(".", "")))
                                    .year2027(Integer.parseInt(splitedRecord.get(14).replace(".", "")))
                                    .year2028(Integer.parseInt(splitedRecord.get(15).replace(".", "")))
                                    .fistFiveYearPercentage(Double.parseDouble(splitedRecord.get(16).replace("%", "")))
                                    .secondFiveYearPercentage(Double.parseDouble(splitedRecord.get(17).replace("%", "")))
                                    .decadePercentage(Double.parseDouble(splitedRecord.get(18).replace("%", "")))
                                    .build();
                        }
                        return forecasting;
                    } else {
                        String[] splitedRecord = record.split(",");
                        if (splitedRecord.length == br.getValue()) {
                            forecasting = Forecasting.builder()
                                    .NOC(splitedRecord[0])
                                    .description(splitedRecord[1])
                                    .industry(splitedRecord[2])
                                    .variable(splitedRecord[3])
                                    .geografickArea(splitedRecord[4])
                                    .year2018(Integer.parseInt(splitedRecord[5].replace(".", "")))
                                    .year2019(Integer.parseInt(splitedRecord[6].replace(".", "")))
                                    .year2020(Integer.parseInt(splitedRecord[7].replace(".", "")))
                                    .year2021(Integer.parseInt(splitedRecord[8].replace(".", "")))
                                    .year2022(Integer.parseInt(splitedRecord[9].replace(".", "")))
                                    .year2023(Integer.parseInt(splitedRecord[10].replace(".", "")))
                                    .year2024(Integer.parseInt(splitedRecord[11].replace(".", "")))
                                    .year2025(Integer.parseInt(splitedRecord[12].replace(".", "")))
                                    .year2026(Integer.parseInt(splitedRecord[13].replace(".", "")))
                                    .year2027(Integer.parseInt(splitedRecord[14].replace(".", "")))
                                    .year2028(Integer.parseInt(splitedRecord[15].replace(".", "")))
                                    .fistFiveYearPercentage(Double.parseDouble(splitedRecord[16].replace("%", "")))
                                    .secondFiveYearPercentage(Double.parseDouble(splitedRecord[17].replace("%", "")))
                                    .decadePercentage(Double.parseDouble(splitedRecord[18].replace("%", "")))
                                    .build();
                        }
                        return forecasting;
                    }
                })
                .filter(forecasting -> forecasting!=null)
                .foreachPartition(records -> {
                    KafkaProducer<String, Object> producer = new KafkaProducer<String, Object>(kafkaParams);
                    schema=ReflectData.get().getSchema(Forecasting.class);
                    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                    while (records.hasNext()) {
                        Thread.sleep(500);
                        GenericRecord genericRecord=new GenericData.Record(schema);
                        genericRecord.put("NOC",records.next().getNOC());
                        genericRecord.put("description",records.next().getDescription());
                        genericRecord.put("industry",records.next().getIndustry());
                        genericRecord.put("variable",records.next().getVariable());
                        genericRecord.put("geografickArea",records.next().getGeografickArea());
                        genericRecord.put("year2018",records.next().getYear2018());
                        genericRecord.put("year2019",records.next().getYear2019());
                        genericRecord.put("year2020",records.next().getYear2020());
                        genericRecord.put("year2021",records.next().getYear2021());
                        genericRecord.put("year2022",records.next().getYear2022());
                        genericRecord.put("year2023",records.next().getYear2023());
                        genericRecord.put("year2024",records.next().getYear2024());
                        genericRecord.put("year2025",records.next().getYear2025());
                        genericRecord.put("year2026",records.next().getYear2026());
                        genericRecord.put("year2027",records.next().getYear2027());
                        genericRecord.put("year2028",records.next().getYear2028());
                        genericRecord.put("fistFiveYearPercentage",records.next().getFistFiveYearPercentage());
                        genericRecord.put("secondFiveYearPercentage",records.next().getSecondFiveYearPercentage());
                        genericRecord.put("decadePercentage",records.next().getDecadePercentage());
                        producer.send(new ProducerRecord<>("test", recordInjection.apply(genericRecord)));
                    }
                });
//                    .filter(x->!x.contains("№"))
//                .filter(x->x.contains("подлинник"))
//                .map(x->{
//                    String[] strings = x.split(";");
//                    Entrant entrant;
//                    try {
//                        entrant = Entrant.builder()
//                                .fio(strings[2])
//                                .xim(Integer.parseInt(strings[3]))
//                                .bio(Integer.parseInt(strings[4]))
//                                .rus(Integer.parseInt(strings[6]))
//                                .individualArchivement(Integer.parseInt(strings[7]))
//                                .build();
//                    }
//                    catch (Exception e){
//                        entrant = Entrant.builder()
//                                .fio(strings[2])
//                                .xim(0)
//                                .bio(0)
//                                .rus(0)
//                                .individualArchivement(0)
//                                .build();
//                    }
//                    return entrant;
//                })
//                .sortBy(entrant -> (entrant.getBio()+entrant.getRus()+entrant.getXim()+entrant.getIndividualArchivement()),false,1)
//                .take(15);
//        entrants.stream()
//                .forEach(entrant->
//                        System.out.println(entrant.getFio()+" "+(entrant.getXim()+entrant.getBio()+entrant.getRus()+entrant.getIndividualArchivement()))
//                );
    }
}
