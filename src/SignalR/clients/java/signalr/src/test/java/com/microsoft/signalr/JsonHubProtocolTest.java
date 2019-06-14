// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

package com.microsoft.signalr;

import com.microsoft.signalr.interfaces.ConfigFetchingController;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;


class JsonHubProtocolTest {
    private JsonHubProtocol gsonHubProtocol = new JsonHubProtocol(new ConfigFetchingController() {
        @Override
        public JsonConverterType jsonConverterType() {
            return JsonConverterType.GSON;
        }
    });
    private JsonHubProtocol jacksonHubProtocol = new JsonHubProtocol(new ConfigFetchingController() {
        @Override
        public JsonConverterType jsonConverterType() {
            return JsonConverterType.JACKSON;
        }
    });

    @Test
    public void checkGsonProtocolName() {
        assertEquals("json", gsonHubProtocol.getName());
    }

    @Test
    public void checkJacksonProtocolName() {
        assertEquals("json", jacksonHubProtocol.getName());
    }

    @Test
    public void checkGsonVersionNumber() {
        assertEquals(1, gsonHubProtocol.getVersion());
    }

    @Test
    public void checkJacksonVersionNumber() {
        assertEquals(1, jacksonHubProtocol.getVersion());
    }

    @Test
    public void checkGsonTransferFormat() {
        assertEquals(TransferFormat.TEXT, jacksonHubProtocol.getTransferFormat());
    }

    @Test
    public void checkJacksonTransferFormat() {
        assertEquals(TransferFormat.TEXT, jacksonHubProtocol.getTransferFormat());
    }

    private void verifyJsonWriteMessage(JsonHubProtocol jsonHubProtocol) {
        InvocationMessage invocationMessage = new InvocationMessage(null, "test", new Object[] {"42"});
        String result = jsonHubProtocol.writeMessage(invocationMessage);
        String expectedResult = "{\"type\":1,\"target\":\"test\",\"arguments\":[\"42\"]}\u001E";
        assertEquals(expectedResult, result);
    }

    @Test
    public void verifyGsonWriteMessage() {
        verifyJsonWriteMessage(gsonHubProtocol);
    }

    @Test
    public void verifyJacksonWriteMessage() {
        verifyJsonWriteMessage(jacksonHubProtocol);
    }

    private void parsePintMessage(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":6}\u001E";
        TestBinder binder = new TestBinder(PingMessage.getInstance());

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        //We know it's only one message
        assertEquals(1, messages.length);
        assertEquals(HubMessageType.PING, messages[0].getMessageType());
    }

    @Test
    public void parseGsonPingMessage() {
        parsePintMessage(gsonHubProtocol);
    }

    @Test
    public void parseJacksonPingMessage() {
        parsePintMessage(jacksonHubProtocol);
    }

    private void parseCloseMessage(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":7}\u001E";
        TestBinder binder = new TestBinder(new CloseMessage());

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        //We know it's only one message
        assertEquals(1, messages.length);

        assertEquals(HubMessageType.CLOSE, messages[0].getMessageType());

        //We can safely cast here because we know that it's a close message.
        CloseMessage closeMessage = (CloseMessage) messages[0];

        assertEquals(null, closeMessage.getError());
    }

    @Test
    public void parseGsonCloseMessage() {
        parseCloseMessage(gsonHubProtocol);
    }

    @Test
    public void parseJacksonCloseMessage() {
        parseCloseMessage(jacksonHubProtocol);
    }

    private void parseCloseMessageWithError(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":7,\"error\": \"There was an error\"}\u001E";
        TestBinder binder = new TestBinder(new CloseMessage("There was an error"));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        //We know it's only one message
        assertEquals(1, messages.length);

        assertEquals(HubMessageType.CLOSE, messages[0].getMessageType());

        //We can safely cast here because we know that it's a close message.
        CloseMessage closeMessage = (CloseMessage) messages[0];

        assertEquals("There was an error", closeMessage.getError());
    }

    @Test
    public void parseGsonCloseMessageWithError() {
        parseCloseMessageWithError(gsonHubProtocol);
    }

    @Test
    public void parseJacksonCloseMessageWithError() {
        parseCloseMessageWithError(jacksonHubProtocol);
    }

    private void parseSingleMessage(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":[42]}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage("1", "test", new Object[] { 42 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        //We know it's only one message
        assertEquals(1, messages.length);

        assertEquals(HubMessageType.INVOCATION, messages[0].getMessageType());

        //We can safely cast here because we know that it's an invocation message.
        InvocationMessage invocationMessage = (InvocationMessage) messages[0];

        assertEquals("test", invocationMessage.getTarget());
        assertEquals(null, invocationMessage.getInvocationId());

        int messageResult = (int)invocationMessage.getArguments()[0];
        assertEquals(42, messageResult);
    }

    @Test
    public void parseGsonSingleMessage() {
        parseSingleMessage(gsonHubProtocol);
    }

    @Test
    public void parseJacksonSingleMessage() {
        parseSingleMessage(jacksonHubProtocol);
    }

    private void parseSingleUnsupportedStreamInvocationMessage(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":4,\"Id\":1,\"target\":\"test\",\"arguments\":[42]}\u001E";
        TestBinder binder = new TestBinder(new StreamInvocationMessage("1", "test", new Object[] { 42 }));

        Throwable exception = assertThrows(UnsupportedOperationException.class, () -> jsonHubProtocol.parseMessages(stringifiedMessage, binder));
        assertEquals("The message type STREAM_INVOCATION is not supported yet.", exception.getMessage());
    }

    @Test
    public void parseGsonSingleUnsupportedStreamInvocationMessage() {
        parseSingleUnsupportedStreamInvocationMessage(gsonHubProtocol);
    }

    @Test
    public void parseJacksonSingleUnsupportedStreamInvocationMessage() {
        parseSingleUnsupportedStreamInvocationMessage(jacksonHubProtocol);
    }

    public void parseSingleUnsupportedCancelInvocationMessage(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":5,\"invocationId\":123}\u001E";
        TestBinder binder = new TestBinder(null);

        Throwable exception = assertThrows(UnsupportedOperationException.class, () -> jsonHubProtocol.parseMessages(stringifiedMessage, binder));
        assertEquals("The message type CANCEL_INVOCATION is not supported yet.", exception.getMessage());
    }

    @Test
    public void parseGsonSingleUnsupportedCancelInvocationMessage() {
        parseSingleUnsupportedCancelInvocationMessage(gsonHubProtocol);
    }

    @Test
    public void parseJacksonSingleUnsupportedCancelInvocationMessage() {
        parseSingleUnsupportedCancelInvocationMessage(jacksonHubProtocol);
    }

    public void parseTwoMessages(JsonHubProtocol jsonHubProtocol) {
        String twoMessages = "{\"type\":1,\"target\":\"one\",\"arguments\":[42]}\u001E{\"type\":1,\"target\":\"two\",\"arguments\":[43]}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage("1", "one", new Object[] { 42 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(twoMessages, binder);
        assertEquals(2, messages.length);

        // Check the first message
        assertEquals(HubMessageType.INVOCATION, messages[0].getMessageType());

        //Now that we know we have an invocation message we can cast the hubMessage.
        InvocationMessage invocationMessage = (InvocationMessage) messages[0];

        assertEquals("one", invocationMessage.getTarget());
        assertEquals(null, invocationMessage.getInvocationId());
        int messageResult = (int)invocationMessage.getArguments()[0];
        assertEquals(42, messageResult);

        // Check the second message
        assertEquals(HubMessageType.INVOCATION, messages[1].getMessageType());

        //Now that we know we have an invocation message we can cast the hubMessage.
        InvocationMessage invocationMessage2 = (InvocationMessage) messages[1];

        assertEquals("two", invocationMessage2.getTarget());
        assertEquals(null, invocationMessage2.getInvocationId());
        int secondMessageResult = (int)invocationMessage2.getArguments()[0];
        assertEquals(43, secondMessageResult);
    }

    @Test
    public void parseGsonTwoMessages() {
        parseTwoMessages(gsonHubProtocol);
    }

    @Test
    public void parseJacksonTwoMessages() {
        parseTwoMessages(jacksonHubProtocol);
    }

    public void parseSingleMessageMultipleArgs(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":[42, 24]}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage("1", "test", new Object[] { 42, 24 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        //We know it's only one message
        assertEquals(HubMessageType.INVOCATION, messages[0].getMessageType());

        InvocationMessage message = (InvocationMessage)messages[0];
        assertEquals("test", message.getTarget());
        assertEquals(null, message.getInvocationId());
        int messageResult = (int) message.getArguments()[0];
        int messageResult2 = (int) message.getArguments()[1];
        assertEquals(42, messageResult);
        assertEquals(24, messageResult2);
    }

    @Test
    public void parseGsonSingleMessageMultipleArgs() {
        parseSingleMessageMultipleArgs(gsonHubProtocol);
    }

    @Test
    public void parseJacksonSingleMessageMultipleArgs() {
        parseSingleMessageMultipleArgs(jacksonHubProtocol);
    }

    public void parseMessageWithOutOfOrderProperties(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"arguments\":[42, 24],\"type\":1,\"target\":\"test\"}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage("1", "test", new Object[] { 42, 24 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        // We know it's only one message
        assertEquals(HubMessageType.INVOCATION, messages[0].getMessageType());

        InvocationMessage message = (InvocationMessage) messages[0];
        assertEquals("test", message.getTarget());
        assertEquals(null, message.getInvocationId());
        int messageResult = (int) message.getArguments()[0];
        int messageResult2 = (int) message.getArguments()[1];
        assertEquals(42, messageResult);
        assertEquals(24, messageResult2);
    }

    @Test
    public void parseGsonMessageWithOutOfOrderProperties() {
        parseMessageWithOutOfOrderProperties(gsonHubProtocol);
    }

    @Test
    public void parseJacksonMessageWithOutOfOrderProperties() {
        parseMessageWithOutOfOrderProperties(jacksonHubProtocol);
    }

    public void parseCompletionMessageWithOutOfOrderProperties(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":3,\"result\":42,\"invocationId\":\"1\"}\u001E";
        TestBinder binder = new TestBinder(new CompletionMessage("1", 42, null));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);

        // We know it's only one message
        assertEquals(HubMessageType.COMPLETION, messages[0].getMessageType());

        CompletionMessage message = (CompletionMessage) messages[0];
        assertEquals(null, message.getError());
        assertEquals(42 , message.getResult());
    }

    @Test
    public void parseGsonCompletionMessageWithOutOfOrderProperties() {
        parseCompletionMessageWithOutOfOrderProperties(gsonHubProtocol);
    }

    @Test
    public void parseJacksonCompletionMessageWithOutOfOrderProperties() {
        parseCompletionMessageWithOutOfOrderProperties(jacksonHubProtocol);
    }

    public void invocationBindingFailureWhileParsingTooManyArgumentsWithOutOfOrderProperties(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"arguments\":[42, 24],\"type\":1,\"target\":\"test\"}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage(null, "test", new Object[] { 42 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);
        assertEquals(1, messages.length);
        assertEquals(InvocationBindingFailureMessage.class, messages[0].getClass());
        InvocationBindingFailureMessage message = (InvocationBindingFailureMessage)messages[0];
        assertEquals("Invocation provides 2 argument(s) but target expects 1.", message.getException().getMessage());
    }

    @Test
    public void invocationGsonBindingFailureWhileParsingTooManyArgumentsWithOutOfOrderProperties() {
        invocationBindingFailureWhileParsingTooManyArgumentsWithOutOfOrderProperties(gsonHubProtocol);
    }

    @Test
    public void invocationJacksonBindingFailureWhileParsingTooManyArgumentsWithOutOfOrderProperties() {
        invocationBindingFailureWhileParsingTooManyArgumentsWithOutOfOrderProperties(jacksonHubProtocol);
    }

    public void invocationBindingFailureWhileParsingTooManyArguments(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":[42, 24]}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage(null, "test", new Object[] { 42 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);
        assertEquals(1, messages.length);
        assertEquals(InvocationBindingFailureMessage.class, messages[0].getClass());
        InvocationBindingFailureMessage message = (InvocationBindingFailureMessage) messages[0];
        assertEquals("Invocation provides 2 argument(s) but target expects 1.", message.getException().getMessage());
    }

    @Test
    public void invocationGsonBindingFailureWhileParsingTooManyArguments() {
        invocationBindingFailureWhileParsingTooManyArguments(gsonHubProtocol);
    }

    @Test
    public void invocationJacksonBindingFailureWhileParsingTooManyArguments() {
        invocationBindingFailureWhileParsingTooManyArguments(jacksonHubProtocol);
    }

    public void invocationBindingFailureWhileParsingTooFewArguments(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":[42]}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage(null, "test", new Object[] { 42, 24 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);
        assertEquals(1, messages.length);
        assertEquals(InvocationBindingFailureMessage.class, messages[0].getClass());
        InvocationBindingFailureMessage message = (InvocationBindingFailureMessage) messages[0];
        assertEquals("Invocation provides 1 argument(s) but target expects 2.", message.getException().getMessage());
    }

    @Test
    public void invocationGsonBindingFailureWhileParsingTooFewArguments() {
        invocationBindingFailureWhileParsingTooFewArguments(gsonHubProtocol);
    }

    @Test
    public void invocationJacksonBindingFailureWhileParsingTooFewArguments() {
        invocationBindingFailureWhileParsingTooFewArguments(jacksonHubProtocol);
    }

    public void invocationBindingFailureWhenParsingIncorrectType(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":[\"true\"]}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage(null, "test", new Object[] { 42 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);
        assertEquals(1, messages.length);
        assertEquals(InvocationBindingFailureMessage.class, messages[0].getClass());
        InvocationBindingFailureMessage message = (InvocationBindingFailureMessage) messages[0];
        assertEquals("java.lang.NumberFormatException: For input string: \"true\"", message.getException().getMessage());
    }

    @Test
    public void invocationGsonBindingFailureWhenParsingIncorrectType() {
        invocationBindingFailureWhenParsingIncorrectType(gsonHubProtocol);
    }

    @Test
    public void invocationJacksonBindingFailureWhenParsingIncorrectType() {
        invocationBindingFailureWhenParsingIncorrectType(jacksonHubProtocol);
    }

    public void invocationBindingFailureStillReadsJsonPayloadAfterFailure(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":[\"true\"],\"invocationId\":\"123\"}\u001E";
        TestBinder binder = new TestBinder(new InvocationMessage(null, "test", new Object[] { 42 }));

        HubMessage[] messages = jsonHubProtocol.parseMessages(stringifiedMessage, binder);
        assertEquals(1, messages.length);
        assertEquals(InvocationBindingFailureMessage.class, messages[0].getClass());
        InvocationBindingFailureMessage message = (InvocationBindingFailureMessage) messages[0];
        assertEquals("java.lang.NumberFormatException: For input string: \"true\"", message.getException().getMessage());
        assertEquals("123", message.getInvocationId());
    }

    @Test
    public void invocationGsonBindingFailureStillReadsJsonPayloadAfterFailure() {
        invocationBindingFailureStillReadsJsonPayloadAfterFailure(gsonHubProtocol);
    }

    @Test
    public void invocationJacksonBindingFailureStillReadsJsonPayloadAfterFailure() {
        invocationBindingFailureStillReadsJsonPayloadAfterFailure(jacksonHubProtocol);
    }

    public void errorWhileParsingIncompleteMessage(JsonHubProtocol jsonHubProtocol) {
        String stringifiedMessage = "{\"type\":1,\"target\":\"test\",\"arguments\":";
        TestBinder binder = new TestBinder(new InvocationMessage(null, "test", new Object[] { 42, 24 }));

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> jsonHubProtocol.parseMessages(stringifiedMessage, binder));
        assertEquals("Message is incomplete.", exception.getMessage());
    }

    @Test
    public void errorGsonWhileParsingIncompleteMessage() {
        errorWhileParsingIncompleteMessage(gsonHubProtocol);
    }

    @Test
    public void errorJacksonWhileParsingIncompleteMessage() {
        errorWhileParsingIncompleteMessage(jacksonHubProtocol);
    }

    private class TestBinder implements InvocationBinder {
        private Class<?>[] paramTypes = null;
        private Class<?> returnType = null;

        public TestBinder(HubMessage expectedMessage) {
            if (expectedMessage == null) {
                return;
            }

            switch (expectedMessage.getMessageType()) {
                case STREAM_INVOCATION:
                    ArrayList<Class<?>> streamTypes = new ArrayList<>();
                    for (Object obj : ((StreamInvocationMessage) expectedMessage).getArguments()) {
                        streamTypes.add(obj.getClass());
                    }
                    paramTypes = streamTypes.toArray(new Class<?>[streamTypes.size()]);
                    break;
                case INVOCATION:
                    ArrayList<Class<?>> types = new ArrayList<>();
                    for (Object obj : ((InvocationMessage) expectedMessage).getArguments()) {
                        types.add(obj.getClass());
                    }
                    paramTypes = types.toArray(new Class<?>[types.size()]);
                    break;
                case STREAM_ITEM:
                    break;
                case COMPLETION:
                    returnType = ((CompletionMessage)expectedMessage).getResult().getClass();
                    break;
                default:
                    break;
            }
        }

        @Override
        public Class<?> getReturnType(String invocationId) {
            return returnType;
        }

        @Override
        public List<Class<?>> getParameterTypes(String methodName) {
            if (paramTypes == null) {
                return new ArrayList<>();
            }
            return new ArrayList<Class<?>>(Arrays.asList(paramTypes));
        }
    }
}
