/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.webdav;

import org.apache.nifi.processors.webdav.MockSardine.MockDavResource;
import org.apache.nifi.processors.webdav.MockSardine.MockSardine;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URISyntaxException;
import java.util.Date;


public class ListWebDAVTest {

    private TestRunner testRunner;

    @Before
    public void init() throws URISyntaxException {
        // start a dummy webdav server
        MockSardine mockSardine = new MockSardine();
        ListWebDAV test = Mockito.spy(new ListWebDAV());
        Mockito.doReturn(mockSardine).when(test).buildSardine();
        mockSardine.addResource(new MockDavResource("/test/file1", new Date(1000), "mime/test", 1L));
        mockSardine.addResource(new MockDavResource("/test/file2", new Date(1000), "mime/test", 1L));
        testRunner = TestRunners.newTestRunner(test);
    }

    @Test
    public void testProcessor() {
        testRunner.assertNotValid();
        testRunner.setProperty(ListWebDAV.URL, "https://test.com/webdav");
        testRunner.setProperty(ListWebDAV.USERNAME, "test");
        testRunner.setProperty(ListWebDAV.PASSWORD, "test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.assertValid();
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ListWebDAV.REL_SUCCESS);
        testRunner.assertTransferCount(ListWebDAV.REL_SUCCESS, 2);
        testRunner.assertAllConditionsMet(ListWebDAV.REL_SUCCESS, mockFlowFile -> mockFlowFile.getAttribute("path").startsWith("/test/file"));
        testRunner.assertAllConditionsMet(ListWebDAV.REL_SUCCESS, mockFlowFile -> mockFlowFile.getAttribute("filename").startsWith("file"));
    }

}
