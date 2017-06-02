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

//import ycNoise2.Class1;
//import package2.Class1;


public class AirBolt extends BaseRichBolt {
    private OutputCollector collector;


    static private ObjectArray emit_data = null;
    static private ObjectArray step1_data = null;
    static private MWNumericArray n = null;
    static private MWNumericArray th = null;
    static private MWNumericArray region_n = null;
    static private Object[] result_step1_1 = null;
    static private Object[] result_step1_2 = null;
    static private Object[] result_step1_3 = null;
    static private Object[] result_step1_4 = null;
    static private Object[] result_step2 = null;
    static private Object[] result_step2_1 = null;
    static private Object[] result_step2_2 = null;
    static private Object[] result_step2_3 = null;
    static private Object[] result_step3 = null;
    static private Object[] bld3d = null;
    static private AirMap airMap = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        try {

            emit_data = new ObjectArray();
            step1_data = new ObjectArray();

            //System.out.println("*** Step1_Air bolt - Create AirMap ***");
            this.airMap = new AirMap();
            int o=0;
                //System.out.println("region_n = " + o);
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);

            //set Threshold
//            th = new MWNumericArray(Double.valueOf(0.1), MWClassID.DOUBLE);

            result_step1_1 = airMap.step1_1(5);
            result_step1_2 = airMap.step1_2(5);
            result_step1_3 = airMap.step1_3(1, result_step1_2[1], result_step1_2[3], result_step1_2[4],
                    region_n);
//            result_step1_3 = airMap.step1_3(result_step1_2[1], result_step1_2[3], result_step1_2[4],
//                    region_n);

//            result_step2 = airMap.step2(1,region_n);


        }
        catch (Exception e)
        {
            System.out.println("Exception: " + e.toString());
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("step1_bolt"));
    }


    public void execute(Tuple tuple) {
        double beginTime = System.currentTimeMillis();


        try {
            //emit_data = (ObjectArray)tuple.getValueByField("air");
//            emit_data = (ObjectArray) tuple.getValueByField("air_spout");
            emit_data.setValue((Object[])tuple.getValueByField("air_spout"));
            emit_data.setFlag((Integer)(tuple.getValueByField("air_spout1")));
            emit_data.setNum((Integer) tuple.getValueByField("air_spout2"));

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


        }
        catch (Exception e)
        {
            System.out.println("Bolt1 Exception: " + e.toString());
        }

        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the AirBolt took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");
        //collector.ack(tuple);


        }




//    public void execute(Tuple tuple) {
//        String sentence = tuple.getStringByField("sentence");
//        String[] words = sentence.split(" ");
//        for (String word: words) {
//            this.collector.emit(new Values(word));
//        }
//    }
}
