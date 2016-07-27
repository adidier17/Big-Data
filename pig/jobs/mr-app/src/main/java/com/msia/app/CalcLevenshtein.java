
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class CalcLevenshtein extends EvalFunc <Integer> {
	public Integer exec( Tuple input ) throws IOException {
		if ( input == null || input.size() == 0 )
			return null;

		try {
			String word1 = (String) input.get(0);
			String word2 = (String) input.get(1);
            		return Levenshtein.getLevenshteinDistance(word1, word2);
		}
		catch ( Exception e ) {
			throw new IOException( "Caught exception processing input: ", e );
		}
	}
}
