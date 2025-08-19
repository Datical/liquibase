 


package liquibase.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author asales
 */
public class BooleanParserTest {
    
    @Test
    public void testparseBoolean(){
        Assertions.assertFalse(BooleanParser.parseBoolean("-1"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" -1"));
        Assertions.assertFalse(BooleanParser.parseBoolean("-1 "));
        Assertions.assertFalse(BooleanParser.parseBoolean("0"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" 0"));
        Assertions.assertFalse(BooleanParser.parseBoolean("0 "));
        //
        Assertions.assertTrue(BooleanParser.parseBoolean("1"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" 1"));
        Assertions.assertTrue(BooleanParser.parseBoolean("1 "));
        Assertions.assertTrue(BooleanParser.parseBoolean("2"));
        //
        Assertions.assertTrue(BooleanParser.parseBoolean("true"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" true"));
        Assertions.assertTrue(BooleanParser.parseBoolean("true "));
        Assertions.assertTrue(BooleanParser.parseBoolean("True"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" True"));
        Assertions.assertTrue(BooleanParser.parseBoolean("True "));
        Assertions.assertTrue(BooleanParser.parseBoolean("TRUE"));
        Assertions.assertTrue(BooleanParser.parseBoolean("TRUE "));
        Assertions.assertTrue(BooleanParser.parseBoolean(" TRUE"));
        Assertions.assertTrue(BooleanParser.parseBoolean("t"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" t"));
        Assertions.assertTrue(BooleanParser.parseBoolean("t "));
        Assertions.assertTrue(BooleanParser.parseBoolean("T"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" T"));
        Assertions.assertTrue(BooleanParser.parseBoolean("T "));
        Assertions.assertTrue(BooleanParser.parseBoolean("y"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" y"));
        Assertions.assertTrue(BooleanParser.parseBoolean("y "));
        Assertions.assertTrue(BooleanParser.parseBoolean("Y"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" Y"));
        Assertions.assertTrue(BooleanParser.parseBoolean("Y "));
        Assertions.assertTrue(BooleanParser.parseBoolean("yes"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" yes"));
        Assertions.assertTrue(BooleanParser.parseBoolean("yes "));
        Assertions.assertTrue(BooleanParser.parseBoolean("Yes"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" Yes"));
        Assertions.assertTrue(BooleanParser.parseBoolean("Yes "));
        Assertions.assertTrue(BooleanParser.parseBoolean("YES"));
        Assertions.assertTrue(BooleanParser.parseBoolean(" YES"));
        Assertions.assertTrue(BooleanParser.parseBoolean("YES "));
        //
        
        Assertions.assertFalse(BooleanParser.parseBoolean("false"));
        Assertions.assertFalse(BooleanParser.parseBoolean("false "));
        Assertions.assertFalse(BooleanParser.parseBoolean(" false"));
        Assertions.assertFalse(BooleanParser.parseBoolean("False"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" False"));
        Assertions.assertFalse(BooleanParser.parseBoolean("False "));
        Assertions.assertFalse(BooleanParser.parseBoolean("FALSE"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" FALSE"));
        Assertions.assertFalse(BooleanParser.parseBoolean("FALSE "));
        Assertions.assertFalse(BooleanParser.parseBoolean("f"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" f"));
        Assertions.assertFalse(BooleanParser.parseBoolean("f "));
        Assertions.assertFalse(BooleanParser.parseBoolean("F"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" F"));
        Assertions.assertFalse(BooleanParser.parseBoolean("F "));
        Assertions.assertFalse(BooleanParser.parseBoolean("n"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" n"));
        Assertions.assertFalse(BooleanParser.parseBoolean("n "));
        Assertions.assertFalse(BooleanParser.parseBoolean("N"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" N"));
        Assertions.assertFalse(BooleanParser.parseBoolean("N "));
        Assertions.assertFalse(BooleanParser.parseBoolean("no"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" no"));
        Assertions.assertFalse(BooleanParser.parseBoolean("no "));
        Assertions.assertFalse(BooleanParser.parseBoolean("No"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" No"));
        Assertions.assertFalse(BooleanParser.parseBoolean("No "));
        Assertions.assertFalse(BooleanParser.parseBoolean("NO"));
        Assertions.assertFalse(BooleanParser.parseBoolean(" NO"));
        Assertions.assertFalse(BooleanParser.parseBoolean("NO "));
        
        String test = null;
        Assertions.assertFalse(BooleanParser.parseBoolean(test));
        Assertions.assertFalse(BooleanParser.parseBoolean(" any dummy text!"));
    }
    
}
