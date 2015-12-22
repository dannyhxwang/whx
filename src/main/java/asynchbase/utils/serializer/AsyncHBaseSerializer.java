/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package asynchbase.utils.serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface represent a serializer
 * capable of returning a byte array from
 * a specific object to be stored to from HBase.
 */
public interface AsyncHBaseSerializer extends Serializable {
    /**
     * @param object Object to serialize
     * @return A byte array.
     */
    byte[] serialize(Object object);

    /**
     * <p>
     * Initialize the serializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}