dedup_syslog
=========

Syslog Data Deduplication

At my past two jobs, anywhere from 40GB to 60GB would be sent to syslog servers.  Raw compression is one solution, but expensive.  Given the huge amount of duplicate data in syslog traffic, I believe data dedpulication to be the optimal solution.  I chose to use a Redis server, enabling both clients and server to share indexes.  And berkeleyDB for local cache.  In a future release, I plan to use sqlite to enable cheap fuzzy matches.
