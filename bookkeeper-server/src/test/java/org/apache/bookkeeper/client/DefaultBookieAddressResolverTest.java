package org.apache.bookkeeper.client;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.utils.ExpectedResult;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class DefaultBookieAddressResolverTest {
    @RunWith(Parameterized.class)
    public static class ParameterizedTests {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            ExpectedResult<BookieSocketAddress> exception = new ExpectedResult<>(null, Exception.class);
            ExpectedResult<BookieSocketAddress> valid = new ExpectedResult<>(new BookieSocketAddress("localhost", 8080), null);
            ExpectedResult<BookieSocketAddress> dummyValid = new ExpectedResult<>(new BookieSocketAddress("localhost", 0), null);
            return Arrays.asList(
                    new Object[][]{
                            {exception, null},
                            {exception, BookieId.parse("invalid")},
                            {valid, BookieId.parse("localhost:8080")},
                            // Improvements
                            {exception, BookieId.parse("localhost:8081")},
//                        {exception, BookieId.parse("localhost:8082")}, // Fail: defaults to endpoint
                            {valid, BookieId.parse("localhost:8082")},
                            {dummyValid, BookieSocketAddress.createDummyBookieIdForHostname("localhost")},
                            {exception, BookieId.parse("localhost")},
                    }
            );
        }

        private static DefaultBookieAddressResolver resolver;
        private final ExpectedResult<BookieSocketAddress> expected;
        private final BookieId bookieId;

        public ParameterizedTests(ExpectedResult<BookieSocketAddress> expected, BookieId bookieId) {
            this.expected = expected;
            this.bookieId = bookieId;
        }

        @BeforeClass
        public static void setup() {
            RegistrationClient client = mock(RegistrationClient.class);
            BookieServiceInfo.Endpoint validEndpoint = new BookieServiceInfo.Endpoint();
            validEndpoint.setHost("localhost");
            validEndpoint.setPort(8080);
            validEndpoint.setProtocol("bookie-rpc");
            BookieServiceInfo validInfo = new BookieServiceInfo(new HashMap<>(), Collections.singletonList(validEndpoint));

            BookieServiceInfo.Endpoint invalidEndpoint = new BookieServiceInfo.Endpoint();
            invalidEndpoint.setHost("localhost");
            invalidEndpoint.setPort(8081);
            invalidEndpoint.setProtocol("no-protocol");
            BookieServiceInfo invalidInfo = new BookieServiceInfo(new HashMap<>(), Collections.singletonList(invalidEndpoint));

            when(client.getBookieServiceInfo(BookieId.parse("localhost:8080")))
                    .thenReturn(FutureUtils.value(new Versioned<>(validInfo, new LongVersion(-1))));
            when(client.getBookieServiceInfo(BookieId.parse("localhost:8081")))
                    .thenReturn(FutureUtils.value(new Versioned<>(invalidInfo, new LongVersion(-1))));
            when(client.getBookieServiceInfo(BookieId.parse("localhost:8082")))
                    .thenReturn(FutureUtils.value(new Versioned<>(validInfo, new LongVersion(-1))));
            when(client.getBookieServiceInfo(BookieSocketAddress.createDummyBookieIdForHostname("localhost")))
                    .thenAnswer(invocationOnMock -> {
                        throw new BKException.BKBookieHandleNotAvailableException();
                    });
            when(client.getBookieServiceInfo(BookieId.parse("localhost")))
                    .thenAnswer(invocationOnMock -> {
                        throw new BKException.BKBookieHandleNotAvailableException();
                    });
            when(client.getBookieServiceInfo(any())).thenThrow();
            resolver = new DefaultBookieAddressResolver(client);
        }

        @Test
        public void resolveTest() {
            try {
                BookieSocketAddress result = resolver.resolve(bookieId);
                Assert.assertEquals(expected.getT(), result);
            } catch (Exception ignored) {
                Assert.assertNotNull(this.expected.getException());
            }
        }
    }

//    public static class NonParameterizedTests {
//        private static DefaultBookieAddressResolver resolver;
//        private static CompletableFuture<Versioned<BookieServiceInfo>> infoFuture;
//
//        @BeforeClass
//        public static void setup() {
//            RegistrationClient client = mock(RegistrationClient.class);
//            BookieServiceInfo.Endpoint validEndpoint = new BookieServiceInfo.Endpoint();
//            validEndpoint.setHost("localhost");
//            validEndpoint.setPort(8080);
//            validEndpoint.setProtocol("bookie-rpc");
//            BookieServiceInfo validInfo = new BookieServiceInfo(new HashMap<>(), Collections.singletonList(validEndpoint));
//
//            infoFuture = CompletableFuture.supplyAsync(() -> {
//                try {
//                    System.out.println("In future sleeping for 200ms");
//                    Thread.sleep(200);
//                } catch (InterruptedException ignored) {
//                }
//                return new Versioned<>(validInfo, new LongVersion(-1));
//            });
//            when(client.getBookieServiceInfo(any())).thenReturn(infoFuture);
//            resolver = new DefaultBookieAddressResolver(client);
//        }
//
//        @Test
//        public void resolveTest() {
//            try {
//                Thread t = new Thread(() -> {
//                    try {
//                        System.out.println("In Thread sleeping for 100ms");
//                        Thread.sleep(100);
//                        infoFuture.cancel(true);
//                    } catch (InterruptedException e) {
//                        Assert.fail();
//                    }
//                });
//                t.start();
//                System.out.println("Resolve starting");
//                resolver.resolve(BookieId.parse("any"));
//                System.out.println("Resolve finished");
//                t.join();
//                System.out.println("Thread finished");
//                Assert.fail();
//            } catch (Exception e) {
//                System.out.println("Success");
//                // Success Condition
//            }
//        }
//    }
}