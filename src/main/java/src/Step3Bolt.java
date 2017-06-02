package src;

import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;


public class Step3Bolt extends BaseRichBolt {
    private OutputCollector collector;
    private int index = 0;

    static private ObjectArray step1_data = null;
    static private ObjectArray step2_data = null;
    static private ObjectArray step2_3_data = null;
    static public MWNumericArray n = null;
    static public MWNumericArray region_n = null;
    static public Object[] result_step1_1 = null;
    static public Object[] result_step1_2 = null;
    static private Object[] result_step1_3 = null;
    static private Object[] result_step1_4 = null;
    static private Object[] result_step2 = null;
    static private Object[] result_step2_1 = null;
    static private Object[] result_step2_2 = null;
    static private Object[] result_step2_3 = null;
    static private Object[] result_step2_4 = null;
    static private Object[] result_step3 = null;
    static public Object[] bld3d = null;
    static public AirMap airMap = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        this.collector = outputCollector;

        try {
            step1_data = new ObjectArray();
            step2_data = new ObjectArray();


            System.out.println("*** result bolt - Create Class1 AirMap ***");
            this.airMap = new AirMap();

            int o=0;
                System.out.println("step3 region_n = " + o);
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);

                result_step1_1 = airMap.step1_1(5);
                // output[5] - xi, yi, grd_spc, gnd_rng, gnd_grd

                result_step1_2 = airMap.step1_2(5);
                // output[2] - nz_rng, cmap
                result_step1_3 = airMap.step1_3(1, result_step1_2[1], result_step1_2[3], result_step1_2[4],
                    region_n);

                 result_step2 = airMap.step2(1,region_n);
                // output[1] - bld3d

            /*airMap.step1_5(region_n);
            airMap.step3(result_step1_1[0],result_step1_1[1],result_step1_1[2],
                    result_step1_1[4],bld3d[0],region_n);
                    */
        }
        catch (Exception e)
        {
            System.out.println("Exception: " + e.toString());
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // not need to create Output Stream;
    }

    public void execute(Tuple tuple) {

        try {
            System.out.println("### Step3 execute start !!! ###");
            step1_data = (ObjectArray)tuple.getValueByField("step1_bolt");
            step2_data = (ObjectArray)tuple.getValueByField("step2_bolt");
            step2_3_data = (ObjectArray)tuple.getValueByField("step2_bolt");
            int o=0;
            System.out.println("step3 region_n = " + o);
            region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);


            if(step1_data.getFlag()==1) {

                //Object[] nz_gnd_gi = emit_data.getValue();
                result_step1_3 = step1_data.getValue();
                System.out.println("### step3 result_step1_3 OK ###");

                result_step1_4 = airMap.step1_4(result_step1_3[0], result_step1_2[3], region_n);


            }
            else if(step2_data.getFlag()==2)
            {
//                result_step2 = step2_data.getValue();
                result_step2_3 = step2_3_data.getValue();
                System.out.println("### step3 result_step2 OK ###");

                for (int i = 0; i < 1; i++) {
                    n = new MWNumericArray(Double.valueOf(51), MWClassID.DOUBLE);
                    region_n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                    result_step2 = airMap.step2(1, region_n);
                    result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                    result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
                    result_step2_4 = airMap.step2_4(result_step2_3[0], result_step2_3[1], result_step2_3[2],
                                result_step2_3[3], result_step2_3[4], region_n, result_step2[0]);
                }

                //airMap.step2_3(result_step1_2[0], result_step1_2[3],result_step2[0],
                //        resu[2],region_n);

            }
            result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
                    result_step1_1[4], result_step2[0], region_n);

        }
        catch (Exception e)
        {
            System.out.println("Step3Bolt Exception: " + e.toString());
        }
        System.out.println("### Step3 Clear ###");

    }

}

