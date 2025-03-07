package org.apache.solr.core;

import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CancellableCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class CancellableQueryTrackerTest {
    private static int expiry;

    @Before
    public void setUp() {
        expiry = CancellableQueryTracker.expiryTimeInSeconds;
        CancellableQueryTracker.expiryTimeInSeconds = 3;
    }

    @After
    public void tearDown() {
        CancellableQueryTracker.expiryTimeInSeconds = expiry;
    }

    @Test
    public void TestGenerateQueryID() {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        Map<String, String[]> args = new HashMap<>();
        LocalSolrQueryRequest sr = new LocalSolrQueryRequest(null, args);
        // if we generate a UUID it shouldn't be our test string
        String uuid1 = tracker.generateQueryID(sr);
        assertNotEquals("myQueryID", uuid1);
        // if we generate another UUID it also should be our test string, or the first UUID
        String uuid2 = tracker.generateQueryID(sr);
        assertNotEquals("myQueryID", uuid2);
        assertNotEquals(uuid1, uuid2);
        // if we set an explicit ID it should be used
        args.put("queryUUID", new String[]{"myQueryID"});
        sr = new LocalSolrQueryRequest(null, args);
        assertEquals("myQueryID", tracker.generateQueryID(sr));
        // but if we try to use it again, it should throw an exception
        Consumer<LocalSolrQueryRequest> localSolrQueryRequestConsumer = (LocalSolrQueryRequest sq) -> {
            try {
                tracker.generateQueryID(sq);
            } catch (IllegalArgumentException e) {
                assertEquals("Duplicate query UUID given", e.getMessage());
                return;
            }
            fail("Should have thrown IllegalArgumentException");
        };
        localSolrQueryRequestConsumer.accept(sr);
        // unless we mark it complete
        tracker.releaseQueryID("myQueryID");
        assertEquals("myQueryID", tracker.generateQueryID(sr));
    }

    @Test
    public void activeCancellableQueryTimesOut() throws InterruptedException {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        CancellableCollector cc = mock(CancellableCollector.class);
        tracker.addShardLevelActiveQuery("myQueryID", cc);
        assertNotNull(tracker.activeCancellableQueries.getIfPresent("myQueryID"));
        TimeUnit.SECONDS.sleep(5);
        assertNull(tracker.activeCancellableQueries.getIfPresent("myQueryID"));
        Mockito.verify(cc.cancel(), Mockito.times(1));
    }
}