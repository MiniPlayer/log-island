/**
 * Copyright (C) 2017 Hurence 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.mailer;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Mailer processor.
 * WARNING: These tests require a real SMTP server so for not having credentials hardcoded here,
 * there are not filled. To run this test:
 * - enable the tests by uncommenting //@Test into @Test for each method
 * - Fill the parameters marked with // <----- FILL ME
 * - For the test to be successful, you should receive in the configured mailbox one mail per test method
 *   (test method is in the mail body). 
 */
public class MailerProcessorTest {
    
    private static Logger logger = LoggerFactory.getLogger(MailerProcessorTest.class);

    /**
     * WARNING: Those values need to be filled so that the test can be run
     */
    private static final String TEST_SMTP_SERVER = "smtp.fillme.com"; // <----- FILL ME
    private static final String TEST_SMTP_PORT = "465"; // <----- FILL ME
    private static final String TEST_SMTP_SECURITY_USERNAME = "fill.me"; // <----- FILL ME
    private static final String TEST_SMTP_SECURITY_PASSWORD = "fillMePassword"; // <----- FILL ME
    private static final String TEST_SMTP_SECURITY_SSL = "true"; // <----- FILL ME
    private static final String TEST_MAIL_FROM_ADDRESS = "tester@logisland.net (must be a valid address!)"; // <----- FILL ME
    private static final String TEST_MAIL_FROM_NAME = "Mailer Processor Tester";
    private static final String TEST_MAIL_BOUNCE_ADDRESS = "bounce@logisland.net (must be a valid address!)"; // <----- FILL ME
    private static final String TEST_MAIL_REPLYTO_ADDRESS = "replyto@logisland.net (must be a valid address!)"; // <----- FILL ME
    private static final String TEST_MAIL_SUBJECT = "Logisland Mailer Processor Test";
    private static final String TEST_MAIL_TO = "tester@logisland.net (must be a valid address where you will receive mails!)"; // <----- FILL ME

    //@Test
    public void testBasicFromConfig() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MailerProcessor());
        testRunner.setProperty(MailerProcessor.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(MailerProcessor.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(MailerProcessor.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(MailerProcessor.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(MailerProcessor.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(MailerProcessor.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(MailerProcessor.MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        testRunner.setProperty(MailerProcessor.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);
        testRunner.setProperty(MailerProcessor.MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        testRunner.setProperty(MailerProcessor.MAIL_SUBJECT, TEST_MAIL_SUBJECT);
        testRunner.setProperty(MailerProcessor.MAIL_TO, TEST_MAIL_TO);
        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(MailerProcessor.FIELD_MAIL_MSG, "testBasicFromConfig:\nThis is the record message.");
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
    
    //@Test
    public void testBasicFromRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MailerProcessor());
        testRunner.setProperty(MailerProcessor.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(MailerProcessor.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(MailerProcessor.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(MailerProcessor.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(MailerProcessor.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(MailerProcessor.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(MailerProcessor.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);
        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(MailerProcessor.FIELD_MAIL_MSG, "testBasicFromRecord:\nThis is the record message.");
        record.setStringField(MailerProcessor.FIELD_MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        record.setStringField(MailerProcessor.FIELD_MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        record.setStringField(MailerProcessor.FIELD_MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        record.setStringField(MailerProcessor.FIELD_MAIL_SUBJECT, TEST_MAIL_SUBJECT);
        record.setStringField(MailerProcessor.FIELD_MAIL_TO, TEST_MAIL_TO);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
}
