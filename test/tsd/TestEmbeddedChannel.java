// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;



import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;


/**
 * <p>Title: TestEmbeddedChannel</p>
 * <p>Description: Test to verify that we can powermock a netty embedded channel</p> 
 * <p><code>net.opentsdb.tsd.TestEmbeddedChannel</code></p>
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})

public class TestEmbeddedChannel {
	
	class FooContainer {
		final Foo innerFoo;		
		FooContainer(final Foo foo) {
			this.innerFoo = foo;
		}
	}
	
	class Foo {
		final FooContainer fooContainer;
		String payload = null;
		
		Foo() {
			fooContainer = new FooContainer(this);
		}
		
		public void write(final String payload) {
			this.payload = payload;
		}
		
		public String readReversed() {
			return payload == null ? null : new StringBuilder(payload).reverse().toString();
		}
	}
	
	@Test
	public void testSpiedFoo() {
		final Foo foo = new Foo();
		final Foo spiedFoo = PowerMockito.spy(foo);
		Assert.assertSame(foo.fooContainer.innerFoo, foo);
		
	}
	
	
	class EchoHandler extends ChannelDuplexHandler {
		final AtomicInteger reads = new AtomicInteger(0);
		@Override
		public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
			reads.incrementAndGet();
			final String value = (String)msg;
			final String response = new StringBuilder(value).reverse().toString();
			ctx.channel().writeAndFlush(response);
		}
	}

	@Test
	public void testEmbeddedChannel() {  // PASSES
		final EchoHandler handler = new EchoHandler();
		final EmbeddedChannel ec = new EmbeddedChannel(handler);
		ec.writeInbound("Hello World");
		final String response = ec.readOutbound();		
		Assert.assertEquals(1, handler.reads.get());
		Assert.assertEquals("dlroW olleH", response);
		
	}
	
	@Test
    public void testSpiedEmbeddedChannel() {  // FAILS
        final EchoHandler handler = new EchoHandler();
        EmbeddedChannel ecx = new EmbeddedChannel(handler);
        EmbeddedChannel ec = spy(ecx);
        
        
        
        org.powermock.reflect.Whitebox.setInternalState(ec.pipeline(), "channel", ecx.pipeline().channel());
        
        org.powermock.reflect.Whitebox.setInternalState(ec.pipeline(), "tail", 
        		org.powermock.reflect.Whitebox.getInternalState(ecx.pipeline(), "tail")
        );
        org.powermock.reflect.Whitebox.setInternalState(ec.pipeline(), "head", 
        		org.powermock.reflect.Whitebox.getInternalState(ecx.pipeline(), "head")
        );
        
        
        ec.writeInbound("Hello World");
        
        final String response = ec.readOutbound();
        verify(ec, times(2)).isOpen();  // OK
        Assert.assertEquals(1, handler.reads.get());  // OK
//        Assert.assertEquals("dlroW olleH", response);  // FAILS
        ec = null;
        ecx = null;
    }
	
	
	@Test
	public void testSpiedEmbeddedChannel2() {  // PASSES WITH WORKAROUND		
		final EchoHandler handler = new EchoHandler();
		final EmbeddedChannel ecx = new EmbeddedChannel(handler);
		EmbeddedChannel ec = spy(ecx);
		ec.writeInbound("Hello World");
		final String response = ecx.readOutbound();		
		verify(ec, times(2)).isOpen();  
		Assert.assertEquals(1, handler.reads.get());  
		Assert.assertEquals("dlroW olleH", response);  
	}
	
	
	
	
}
