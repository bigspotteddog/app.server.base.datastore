package com.maintainer.data.provider.datastore;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class LocalServiceTest extends TestCase {

    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), new LocalBlobstoreServiceTestConfig());

    public LocalServiceTest() {}

    public LocalServiceTest(final String name) {
        super(name);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        helper.setUp();
    }

    @Override
    @After
    public void tearDown() {
        helper.tearDown();
    }
}
