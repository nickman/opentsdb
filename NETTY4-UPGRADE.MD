Port of OpenTSDB from Netty 3 to Netty 4.1
==========================================
Changes in Netty
 - Pooled/Unpooled, Heap/Direct 
Changes in Core OpenTSDB
Additions to Core OpenTSDB
 - TSDServer / Configs
 - BufferManager / Configs
 - UnixSocket Server / Configs - no detect, telnet iface only. make sure shutdown is available.
Changes and Additions to OpenTSDB Test Suite
 - Why the old tests broke and how they're fixed
 - Mods and new stuff in NettyMocks 
 
 
-- Move UnixSocketServer into pipeline.
-- Implement connection limits
-- Re-enable idle timeout handler
-- Ref to TSDServer for shutdown
-- TSDServer and UnixSocketServer stats collect
-- Add new parameters to conf.json and site docs.


-- Other issues
-- Don't require gnuplot if tsd is WO.
-- 