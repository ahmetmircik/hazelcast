package com.hazelcast.disk;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 12/20/13
 */
public interface PersistencyUnit<Unit> {

    Unit createNew() throws IOException;

}
