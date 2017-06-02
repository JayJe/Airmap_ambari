package src;

import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

//import ycNoise2.Class1;
//import package2.Class1;

public class AirSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    static private ObjectArray emit_data = null;
    static private ObjectArray emit_data1 = null;
    static private ObjectArray step2_data = null;
    static private MWNumericArray n = null;
    static private MWNumericArray region_n = null;
    Object[] result_step1_1 = null;
    Object[] result_step1_2 = null;
    Object[] result_step2 = null;
    Object[] result_step2_1 = null;
    Object[] result_step2_2 = null;
    Object[] result_step2_3 = null;
    Object[] result_step1_3 = null;
    Object[] bld3d = null;
    AirMap airMap;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            emit_data = new ObjectArray();
            emit_data1 = new ObjectArray();
            step2_data = new ObjectArray();
//            double beginTime = System.currentTimeMillis();
            airMap = new AirMap();
            for (int o = 0; o <= 1; o++) {
                //System.out.println("region_n = " + o);
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                //System.out.println("### Step1_1 Start ###");
                result_step1_1 = airMap.step1_1(5);
//                System.out.println("### Step1_2 Start ###");
                result_step1_2 = airMap.step1_2(5);
//                System.out.println("### Step1_3 Start ###");
//                result_step1_3 = airMap.step1_3(result_step1_2[1], result_step1_2[3], result_step1_2[4],
//                        region_n);
//                System.out.println("### Step2 Start ###");
                result_step2 = airMap.step2(1, region_n);
                result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
                result_step2_3 = airMap.step2_3(5, result_step1_2[0], result_step1_2[3], result_step2[0]
                        , result_step2_2[0], n, region_n);
            }

        } catch (Exception e) {
            System.out.println("Exception: " + e.toString());
        }
    }



    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("air_spout", "air_spout1", "air_spout2"));
    }

    public void nextTuple() {
        double beginTime = System.currentTimeMillis();


        try {

            for(int i=0; i<1; i++) {

                region_n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                //System.out.println("Step1_1 :  Load Data/gnd_point.txt");
                result_step1_1 = airMap.step1_1(5);
                result_step1_2 = airMap.step1_2(5);

                emit_data.setValue(result_step1_2);
                emit_data.setFlag(1);
                this.collector.emit(new Values(emit_data.getValue(), emit_data.getFlag(), emit_data.getNum()));
            }


                for (int o = 1; o <= 51; o++) {
                    double beginTime_for = System.currentTimeMillis();
                    n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                    //System.out.println("### Step2 Start ###");

                    result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);

                    step2_data.setValue(result_step2_1);
                    step2_data.setFlag(2);
                    step2_data.setNum(o);
                    this.collector.emit(new Values(step2_data.getValue(), step2_data.getFlag(), step2_data.getNum()));
                    Thread.sleep(20 * 1);

                    double endTime_for = System.currentTimeMillis();
                    System.out
                            .println("------------------------------------------------------");
                    System.out.println("#####Making the AirSpout_for statement took " + (endTime_for - beginTime_for) / 1000
                            + " seconds.#####");
                    System.out
                            .println("------------------------------------------------------");

                }
                Thread.sleep(1000 * 300);






//    //        result_step1_1 = airMap.step1_1(5);
//                bld3d = result_step2;
//            //Object[] result_step1_3 = airMap.step1_3(1,region_n);
//                // result_step2_1 = airMap.step2_1(1,region_n);
//
//            emit_data.setValue(bld3d);
//            emit_data.setFlag(2);
//            System.out.println("*** Spout Emit... ***");
//            //this.collector.emit(new Values(result_step1_3[0]));
//            this.collector.emit(new Values(emit_data));

            /////////////////////////////////////////////////// step 1 over

                /*

                for(int k=1; k<=51; k++)
                {
                    n = new MWNumericArray(Double.valueOf(k), MWClassID.DOUBLE);
                    System.out.println("Step2_2 :  Load data/bld_noise.txt");
                    result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                    result_step2_2 = airMap.step2_2(2,result_step2_1[0], 0.1,result_step2[0]);
                    emit_data.setValue(result_step2_2);
                    result_step2_3 = airMap.step2_3(result_step1_2[0], result_step1_2[3], result_step2[0],
                            result_step2_2[0], n, region_n);

                    System.out.println("*** Spout Emit... ***");
                    this.collector.emit(new Values(emit_data));

                    try { Thread.sleep(1000 * 12); } catch (InterruptedException e) { }
                }
                */

        }
        catch (Exception e)
        {
            System.out.println("Spout Exception: " + e.toString());
        }

//        try { Thread.sleep(60*1000); } catch (InterruptedException e) { }

        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the AirSpout took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");
    }


//    public void nextTuple() {
//        this.collector.emit(new Values(sentences[index]));
//        index++;
//        if (index >= sentences.length) {
//            index = 0;
//        }
//        try { Thread.sleep(60); } catch (InterruptedException e) { }
//    }
}
