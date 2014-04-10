package storm.kafka;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.testng.Assert.*;
import org.testng.annotations.*;

/**
 * Date: 12/01/2014
 * Time: 18:09
 */
public class KafkaErrorTest {

    @Test
    public void getError() {
        assertEquals(KafkaError.getError(0), KafkaError.NO_ERROR);
    }

    @Test
    public void offsetMetaDataTooLarge() {
        assertEquals(KafkaError.getError(12), KafkaError.OFFSET_METADATA_TOO_LARGE);
    }

    @Test
    public void unknownNegative() {
        assertEquals(KafkaError.getError(-1), KafkaError.UNKNOWN);
    }

    @Test
    public void unknownPositive() {
        assertEquals(KafkaError.getError(75), KafkaError.UNKNOWN);
    }

    @Test
    public void unknown() {
        assertEquals(KafkaError.getError(13), KafkaError.UNKNOWN);
    }
}
