import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Rate {


    private static final Logger log = LogManager.getLogger(Rate.class);

    public static String CONSUMER_GROUP;
    public static int numberOfPartitions;
    public static AdminClient admin = null;
    public static Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> currentPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> partitionToLag = new HashMap<>();


    static Long sleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> offsets;


    ////WIP TODO
    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();
    public static Instant lastDecision;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    ///////////////////////////////////////////////////////////////////////////


    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static boolean firstIteration= true;



    public static void main(String[] args) throws InterruptedException, ExecutionException {


        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();


        while (true) {
            log.info("New Iteration:");
            getCommittedLatestOffsetsAndLag();
            //computeTotalArrivalRate();

            log.info("Sleeping for {} seconds", sleep / 1000.0);
            Thread.sleep(sleep);


            log.info("End Iteration;");
            log.info("=============================================");
        }

    }


    private static void readEnvAndCrateAdminClient() {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);

    }


    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        //get committed  offsets
        offsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();
        numberOfPartitions = offsets.size();





        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        //initialize consumer to lag to 0
        for (TopicPartition tp : offsets.keySet()) {
            requestLatestOffsets.put(tp, OffsetSpec.latest());
            partitionToLag.put(tp, 0L);

        }


        //blocking call to query latest offset
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            long committedOffset = e.getValue().offset();
            long latestOffset = latestOffsets.get(e.getKey()).offset();
            long lag = latestOffset - committedOffset;


            previousPartitionToCommittedOffset.put(e.getKey(),
                    currentPartitionToCommittedOffset.get(e.getKey()));
            previousPartitionToLastOffset.put(e.getKey(),
                    currentPartitionToLastOffset.get(e.getKey()));



            currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
            currentPartitionToLastOffset.put(e.getKey(), latestOffset);
            partitionToLag.put(e.getKey(), lag);


        }



        ///////////////////////////////////////////////////

        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(Rate.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();


        if(!firstIteration){
            computeTotalArrivalRate();
        }else{
            firstIteration = false;
        }
    }


  /*  private static void computeTotalArrivalRate() throws ExecutionException, InterruptedException {
        BigDecimal totalConsumptionRate;
        BigDecimal totalArrivalRate;

        BigDecimal totalpreviouscommittedoffset =  new BigDecimal(0);
        BigDecimal totalcurrentcommittedoffset = new BigDecimal(0);
        BigDecimal totalpreviousendoffset = new BigDecimal(0);
        BigDecimal totalcurrentendoffset = new BigDecimal(0);
        for (TopicPartition tp : offsets.keySet()) {
            totalpreviouscommittedoffset.add( new BigDecimal( previousPartitionToCommittedOffset.get(tp)));
            totalcurrentcommittedoffset.add(new BigDecimal( currentPartitionToCommittedOffset.get(tp)));
            totalpreviousendoffset.add(new BigDecimal(previousPartitionToLastOffset.get(tp)));
            totalcurrentendoffset.add(new BigDecimal( currentPartitionToLastOffset.get(tp)));
        }


        BigDecimal bdcomitted= (totalcurrentcommittedoffset.subtract(totalpreviouscommittedoffset));
        BigDecimal bdlast= totalcurrentendoffset.subtract(totalpreviousendoffset);



        totalConsumptionRate = bdcomitted.divide(new BigDecimal(sleep));


        totalArrivalRate = bdlast.divide(new BigDecimal(sleep));


      *//*  totalConsumptionRate = ((double) (totalcurrentcommittedoffset - totalpreviouscommittedoffset) / (double)sleep);
        totalArrivalRate = ((double) (totalcurrentendoffset - totalpreviousendoffset) / (double)sleep);*//*

        log.info("totalArrivalRate {}, totalconsumptionRate {}",
                totalArrivalRate.multiply(new BigDecimal(1000.0)), totalConsumptionRate.multiply(new BigDecimal(1000.0)));

   *//*     log.info("time since last up scale decision is {}", Duration.between(lastUpScaleDecision, Instant.now()).toSeconds());
        log.info("time since last down scale decision is {}", Duration.between(lastUpScaleDecision, Instant.now()).toSeconds());*//*

        // youMightWanttoScale(totalArrivalRate);

    }
*/


    private static void computeTotalArrivalRate() throws ExecutionException, InterruptedException {
        double totalConsumptionRate=0;
        double totalArrivalRate =0;

        double [] parrivalrates = new double[offsets.size()];

      /*  long totalpreviouscommittedoffset = 0;
        long totalcurrentcommittedoffset = 0;
        long totalpreviousendoffset = 0;
        long totalcurrentendoffset = 0;
        for (TopicPartition tp : offsets.keySet()) {
            totalpreviouscommittedoffset += previousPartitionToCommittedOffset.get(tp);
            totalcurrentcommittedoffset += currentPartitionToCommittedOffset.get(tp);
            totalpreviousendoffset += previousPartitionToLastOffset.get(tp);
            totalcurrentendoffset += currentPartitionToLastOffset.get(tp);
        }*/


        for (TopicPartition tp : offsets.keySet()) {
            parrivalrates[tp.partition()] = (double)( currentPartitionToLastOffset.get(tp) -previousPartitionToLastOffset.get(tp) )/(double) sleep;
           log.info("Arrival rate into partiiton {} equal {}", tp.partition(),    parrivalrates[tp.partition()]);
        }


        for (TopicPartition tp : offsets.keySet()) {
           totalArrivalRate +=  parrivalrates[tp.partition()];

        }



     /*   totalConsumptionRate = ((double) (totalcurrentcommittedoffset - totalpreviouscommittedoffset) / (double)sleep);
        totalArrivalRate = ((double) (totalcurrentendoffset - totalpreviousendoffset) / (double)sleep);*/

        log.info("totalArrivalRate {}",
                totalArrivalRate * 1000.0);

   /*     log.info("time since last up scale decision is {}", Duration.between(lastUpScaleDecision, Instant.now()).toSeconds());
        log.info("time since last down scale decision is {}", Duration.between(lastUpScaleDecision, Instant.now()).toSeconds());*/

        // youMightWanttoScale(totalArrivalRate);

    }

}
