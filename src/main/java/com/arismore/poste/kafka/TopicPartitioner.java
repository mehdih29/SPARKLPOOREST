package com.arismore.poste.kafka;

/**
 * Created by mehdi on 9/30/15.
 */

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import java.util.Random;

public class TopicPartitioner implements Partitioner {

    public TopicPartitioner(VerifiableProperties props) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        int hashCode;
        if (key == null) {
            hashCode = new Random().nextInt(255);
        } else {
            hashCode = key.hashCode();
        }
        if (numPartitions <= 0) {
            return 0;
        }
        return hashCode % numPartitions;
    }
}
