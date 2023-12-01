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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class DefaultBookieAddressResolverTest {
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

    private final ExpectedResult<BookieSocketAddress> expected;
    private final BookieId bookieId;

    private DefaultBookieAddressResolver resolver;

    public DefaultBookieAddressResolverTest(ExpectedResult<BookieSocketAddress> expected, BookieId bookieId) {
        this.expected = expected;
        this.bookieId = bookieId;
    }

    @Before
    public void setup() {
        RegistrationClient mockClient = mock(RegistrationClient.class);
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

        when(mockClient.getBookieServiceInfo(BookieId.parse("localhost:8080")))
                .thenReturn(FutureUtils.value(new Versioned<>(validInfo, new LongVersion(-1))));
        when(mockClient.getBookieServiceInfo(BookieId.parse("localhost:8081")))
                .thenReturn(FutureUtils.value(new Versioned<>(invalidInfo, new LongVersion(-1))));
        when(mockClient.getBookieServiceInfo(BookieId.parse("localhost:8082")))
                .thenReturn(FutureUtils.value(new Versioned<>(validInfo, new LongVersion(-1))));
        when(mockClient.getBookieServiceInfo(BookieSocketAddress.createDummyBookieIdForHostname("localhost")))
                .thenAnswer(invocationOnMock -> {
                    throw new BKException.BKBookieHandleNotAvailableException();
                });
        when(mockClient.getBookieServiceInfo(BookieId.parse("localhost")))
                .thenAnswer(invocationOnMock -> {
                    throw new BKException.BKBookieHandleNotAvailableException();
                });
//        when(mockClient.getBookieServiceInfo(any())).thenThrow();
        when(mockClient.toString()).thenReturn("MockRegistrationClient");
        resolver = new DefaultBookieAddressResolver(mockClient);
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