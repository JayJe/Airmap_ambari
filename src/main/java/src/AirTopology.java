package src;

import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;


public class AirTopology {


    static public MWNumericArray n = null;
    static public MWNumericArray region_n = null;
    static public Object[] result_step1_1 = null;
    static public Object[] result_step1_2 = null;
    static public Object[] bld3d = null;
    private static AirMap airMap=null;

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        double beginTime = System.currentTimeMillis();


        System.out.println("### Create Topology Builder ###");
        TopologyBuilder builder = new TopologyBuilder();

        /*
        System.out.println("***** 1 *****");
        AirSpout spout = new AirSpout();

        System.out.println("***** 2 *****");
        AirBolt airBolt = new AirBolt();

        System.out.println("***** 3 *****");
        Step3Bolt step3Bolt = new Step3Bolt();


        System.out.println("***** 4 *****");
        */
        //parallelism_hint(Thread) and setNumTask(Component Number)
        builder.setSpout("spout", new AirSpout(), 1); // Spout 연결
        //builder.setBolt(NOISE_BOLT_ID, noiseBolt).shuffleGrouping(AIR_SPOUT_ID); // Spout -> SplitBolt
//        builder.setBolt("step1",new AirBolt(), 2)
//                .shuffleGrouping("spout"); // Spout -> step1
        builder.setBolt("step2", new Step2Bolt(), 37)
                .shuffleGrouping("spout"); // spout -> step2
//        builder.setBolt("Step3", new Step3Bolt())
//                .shuffleGrouping("spout")
//                .shuffleGrouping("step1")
//                .globalGrouping("step2");
//                .shuffleGrouping("step2");



//        LocalCluster cluster = new LocalCluster();
//
//
//        Config config = new Config();
//        config.registerSerialization(ObjectArray.class);
//
//        cluster.submitTopology("AirTopology", config, builder.createTopology());
//
//        try {
//            Thread.sleep(1000 * 660);
//        } catch (InterruptedException e)
//        { } // waiting 10s
//        cluster.killTopology("AirTopology");
//        cluster.shutdown();


        Config conf = new Config();
        conf.setNumAckers(0);
        conf.setNumWorkers(40);
        conf.setMaxSpoutPending(5000);
        StormSubmitter submitter = new StormSubmitter();
//        System.setProperty("storm.jar", "home/honey/Downloads/Airmap_storm_v1/target/Airmap_storm_v1-1.0-SNAPSHOT-jar-with-dependencies.jar");
        submitter.submitTopology("AirTopology", conf, builder.createTopology());
        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the AirTopology took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");
    }

}
