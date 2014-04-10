package stanford.infolab.debugger.utils;

import stanford.infolab.debugger.Scenario.GiraphScenario;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import com.google.protobuf.ByteString;

/**
 * Temporary utility class to generate test protocol buffers that store scenarios.
 * 
 * @author semihsalihoglu
 */
public class GiraphTestScenarioWriter {

    public static void main(String[] args) throws IOException {
    	String outputFileName = args[0];
    	System.out.println("outputFileName: " + outputFileName);

    	// Write a scenario that contains:
    	// 1) vertexID: 89
    	// 2) nbrs: 0, 1, ..., 7
    	// 3) msgs: 0.0, 10.0, 20.0, ..., 50.0
    	GiraphScenario.Builder giraphScenarioBuiler = GiraphScenario.newBuilder();
    	long vertexID = 89L;
    	giraphScenarioBuiler.setVertexId(vertexID);
    	for (int i = 0; i <= 7; ++i) {
    		giraphScenarioBuiler.addNbrs(i);
    	}
    	Text vertexValue = new Text("This is the value of the vertex!!");    	
    	giraphScenarioBuiler.setVertexValue(
    		ByteString.copyFrom(WritableUtils.writeToByteArray(vertexValue)));
    	for (int i = 0; i <= 5; ++i) {
    		DoubleWritable serializedDoubleWritable = new DoubleWritable(i*10.0);
    		byte[] serializedByteArray = WritableUtils.writeToByteArray(serializedDoubleWritable);
    		System.out.println("serializedDoubleWritableByteArray.length: "
    			+ serializedByteArray.length);
    		giraphScenarioBuiler.addMessages(ByteString.copyFrom(serializedByteArray));
    	}
    	GiraphScenario scenario = giraphScenarioBuiler.build();
    	FileOutputStream output = new FileOutputStream(outputFileName);
    	scenario.writeTo(output);
    	output.close();

    	// Deserialize the written pb, and verify that the deserialized scenario contains
    	// the same contents.
    	GiraphScenario deserializedScenario = GiraphScenario.parseFrom(
    		new FileInputStream(outputFileName));
    	System.out.println("deserializedScenario: " + deserializedScenario);
    	System.out.println("deserializedScenario.vertexID: " + deserializedScenario.getVertexId());
    	Text deserializedVertexValue = new Text();
    	WritableUtils.readFieldsFromByteArray(deserializedScenario.getVertexValue().toByteArray(),
    		deserializedVertexValue);
    	System.out.println("deserializedScenario.vertexValue: "
    		+ deserializedVertexValue.toString());
    	for (ByteString msgAsByteString : deserializedScenario.getMessagesList()) {
        	DoubleWritable deserializedDoubleWritable = new DoubleWritable();
        	byte[] byteArray = msgAsByteString.toByteArray();    		
        	System.out.println("deserializedDoubleWritableByteArray.size: " + byteArray.length);
        	WritableUtils.readFieldsFromByteArray(byteArray, deserializedDoubleWritable);
        	System.out.println("Deserialized msg: " + deserializedDoubleWritable.get());
    	}
    	for (long nbrID : deserializedScenario.getNbrsList()) {
    		System.out.println("nbrID: " + nbrID);
    	}
    }
}
