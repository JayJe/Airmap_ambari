package src;

import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by honey on 17. 4. 28.
 */
public class Step2Bolt extends BaseRichBolt {
    private OutputCollector collector;

    static private ObjectArray emit_data = null;
    static private ObjectArray step2_data = null;
    static private ObjectArray step2_3_data = null;
    static private MWNumericArray n = null;
    static private MWNumericArray th = null;
    static private MWNumericArray region_n = null;
    static private ObjectArray step1_data = null;
    static private Object[] result_step1_1 = null;
    static private Object[] result_step1_2 = null;
    static private Object[] result_step1_3 = null;
    static private Object[] result_step1_4 = null;
    static private Object[] result_step2 = null;
    static private Object[] result_step2_1 = null;
    static private Object[] result_step2_2 = null;
    static private Object[] result_step2_3 = null;
    static private Object[] result_step2_4 = null;
    static private Object[] result_step3 = null;
    static private Object[] bld3d = null;
    static private AirMap airMap = null;
    int count = 0;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        try {
            emit_data = new ObjectArray();
            step2_data = new ObjectArray();
            airMap = new AirMap();
            for (int o = 0; o <= 1; o++){
                System.out.println("###step2 region_n ###= " +o);
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                result_step1_1 = airMap.step1_1(5);
                result_step1_2 = airMap.step1_2(5);
                result_step2 = airMap.step2(1,region_n);

//                result_step2 = airMap.step2(1,region_n);
                result_step2_1 = airMap.step2_1(1,result_step2[0], region_n);
//                result_step2_2 = airMap.step2_2(2,result_step2_1[0],0.1,result_step2[0]);
                /*
                for (int i = 1; i <=51; i++){
                    n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                    result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                    result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
                    result_step2_3 = airMap.step2_3(result_step1_2[0], result_step1_2[3], result_step2[0],
                            result_step2_2[0], n, region_n);
                }
                result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
                        result_step1_1[4], result_step2[0], region_n);
                        */

                result_step2_3 = airMap.step2_3(5,result_step1_2[0], result_step1_2[3], result_step2[0]
                        , result_step2_2[0], n, region_n);



            }


        }
        catch (Exception e){
            System.out.println("Step2Bolt Exception : " + e.toString());

        }

    }

    public void execute(Tuple tuple) {
        double beginTime = System.currentTimeMillis();
        try {
//            emit_data1 = (ObjectArray)tuple.getValueByField("air_spout");
//            step2_data = (ObjectArray)tuple.getValues();
//            step2_data = (ObjectArray)tuple.getValueByField("air_spout");
            step2_data.setValue((Object[])tuple.getValueByField("air_spout"));
            step2_data.setFlag((Integer)(tuple.getValueByField("air_spout1")));
            step2_data.setNum((Integer) tuple.getValueByField("air_spout2"));
            System.out.println("### Step2 execute Start ###");
            System.out.println(step2_data.getFlag());
            System.out.println(step2_data.getNum());

            if (emit_data.getFlag() == 1) {

                result_step1_2 = emit_data.getValue();

                // output - nz_gnd
                //System.out.println("**** step1 tuple read ok ****");
                try {

                    emit_data = new ObjectArray();

                    //System.out.println("*** Step1 bolt - Create AirMap ***");
                    this.airMap = new AirMap();
                    int o = 0;

                    //System.out.println("region_n = " + o);
                    region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);

                    result_step1_3 = airMap.step1_3(1, result_step1_2[1], result_step1_2[3], result_step1_2[4],
                            region_n);
                    result_step1_4 = airMap.step1_4(result_step1_3[0], result_step1_2[3], region_n);

//                    result_step2 = airMap.step2(1,region_n);

                    step1_data.setValue(result_step1_3);
                    step1_data.setFlag(1);
                    this.collector.emit(new Values(step1_data));


                } catch (Exception e) {
                    System.out.println("AirBolt Exception: " + e.toString());
                }


                /*Object[] result_step1_4 = airMap.step1_4(1, result_step1_3[0], result_step1_1[0],
                        result_step1_1[1], result_step1_2[0], result_step1_2[1]);


                emit_data.setValue(result_step1_4);

                System.out.println("**** step1_4 tuple emit ready ****");
                this.collector.emit(new Values(emit_data));
                System.out.println("**** step1_4 tuple emit finish ****");
                */
            }

                if (step2_data.getFlag() == 2) {

                    int i = step2_data.getNum();
//                result_step2 = airMap.step2(1, region_n);
//                    emit_data1.setValue(result_step2);
//                    emit_data1.setFlag(2);

                    result_step2_1 = step2_data.getValue();
//                    int i = step2_data.getNum();
                    int o = 0;
                    count++;
                    System.out.println("##################@@@@@@@###########count = "+count);



                        n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE); //
                        System.out.println("########## step2 n = ###########" + i);
                        region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                        result_step2 = airMap.step2(1, region_n);
                        result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
                        result_step2_3 = airMap.step2_3(5, result_step1_2[0], result_step1_2[3], result_step2[0]
                                , result_step2_2[0], n, region_n);
                        result_step2_4 = airMap.step2_4(result_step2_3[0], result_step2_3[1], result_step2_3[2],
                                result_step2_3[3], result_step2_3[4], region_n, result_step2[0]);

//                    for (int i = 1; i <= 51; i++) {
//                        n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
//                        System.out.println("### Step2 Start ###");
//                        result_step2_3 = airMap.step2_3(result_step1_2[0], result_step1_2[3], result_step2[0],
//                                result_step2_2[0], n, region_n);
//                    }
//
                        result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
                                result_step1_1[4], result_step2[0], region_n);

//                        result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
//                                result_step1_1[4], result_step2[0], region_n);



                }

//            step2_data.setValue(result_step2);
//            step2_data.setFlag(2);
//            this.collector.emit(new Values(step2_data));
        }
        catch (Exception e){
            System.out.println("Exception : " + e.toString());
        }
        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the Step2Bolt took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("step2_bolt"));
    }
}
