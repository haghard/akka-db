### Masterless, distributed, replicated, eventually consistent, key-value db on top of rocks db (a toy example)

#Links
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/TransactionSample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/OptimisticTransactionSample.java
https://jepsen.io/consistency/models/snapshot-isolation
http://christophermeiklejohn.com/erlang/lasp/2019/03/08/monotonicity.html


https://www.cockroachlabs.com/blog/consistency-model/
https://www.cockroachlabs.com/blog/cockroachdb-on-rocksd/
https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/


https://fauna.com/blog/a-comparison-of-scalable-database-isolation-levels

https://github.com/justin-db/JustinDB


#Akka-cluster split brain
https://github.com/TanUkkii007/akka-cluster-custom-downing
https://scalac.io/split-brain-scenarios-with-akka-scala/
https://doc.akka.io/docs/akka/2.5.22/typed/actor-discovery.html
Lithium 
https://www.reddit.com/r/scala/comments/evpv5m/lithium_a_splitbrain_resolver_for_akkacluster/
https://speakerdeck.com/dennisvdb/lithium-a-split-brain-resolver-for-akka-cluster 


#Typed actors
https://github.com/hseeberger/welcome-akka-typed/blob/master/src/main/scala/rocks/heikoseeberger/wat/typed/Transfer.scala
https://doc.akka.io/docs/akka/current/typed/routers.html
https://doc.akka.io/docs/akka/current/typed/distributed-data.html
https://github.com/johanandren/akka-typed-samples.git
https://github.com/hseeberger/whirlwind-tour-akka.git


#RocksDB
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/TransactionSample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/OptimisticTransactionSample.java
https://github.com/facebook/rocksdb/wiki/Transactions
https://github.com/facebook/rocksdb/wiki/Merge-Operator
https://github.com/facebook/rocksdb/blob/a283800616cb5da5da43d878037e6398cccf9090/java/src/test/java/org/rocksdb/RocksDBTest.java

https://github.com/lmdbjava/lmdbjava


Apart from transactions, RockDb already has some features that helps you to deal with concurrency
 1. Atomic batch writes (50 keys at once)
 2. Snapshot reads
 3. Merge operations (read/modify/update scenarios by key)


https://rocksdb.org/blog/

https://www.youtube.com/watch?v=aKAJMd0iKtI

https://rockset.com/blog/ivalue-efficient-representation-of-dynamic-types-in-cplusplus/

https://rockset.com/blog/rocksdb-is-eating-the-database-world/

https://rockset.com/blog/index-scan-using-rocksets-search-index-to-speed-up-range-scans/

https://rockset.com/blog/how-we-use-rocksdb-at-rockset/




### Mapping Table Data to Key-Value Storage

https://www.cockroachlabs.com/blog/cockroachdb-on-rocksd/
https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/
https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md



https://github.com/facebook/rocksdb/wiki/Talks



MultiGetForUpdate example: 

https://github.com/facebook/rocksdb/blob/189f0c27aaecdf17ae7fc1f826a423a28b77984f/java/src/test/java/org/rocksdb/OptimisticTransactionTest.java#L96



[RocksDB meetup] Anthony Giardullo, Facebook – RocksDB Transactions
https://www.youtube.com/watch?v=tMeon8FHF3I

### Next Steps

To build a db with snapshot isolation and causal consistency to get referential integrity

Build smth similar to FMK (NHS like system)

https://youtu.be/Vd2I9v3pYpA?t=1282
https://www.youtube.com/watch?v=lO-UfHASUSE
https://www.youtube.com/watch?v=qO9oK7QKbZY
https://www.youtube.com/watch?v=ol1D9X2_nJc
https://www.youtube.com/watch?v=-v_1aJJujdg                      *



Prescription that contains the reference to doctor, patent, pharmacy and medication.
Invariants (statements that always stay true):

AP-compatible invariant: `Relative order`
Referential integrity: a reference to doctor|patient|pharmacy|medication always linked to an existing object.
Implementation: causally related updates always replicated in the same order (like a batch)

AP-compatible invariant: `Joint update`
Atomicity: once I have created a new prescription, all references appear atomically.
Implementation: We need a notion of being able to write all updates atomically. 
The updates are delivered in causal order, the updates inside the transaction are ordered and the batches are applied atomically.    

`Relative order` combined with `Joint update` (transactional causal consistency) is a stronger consistency model for AP systems 

CAP-sensitive  `Precondition-check` (if… then…)
Medication should not be over delivered.


https://codeburst.io/protocol-buffers-part-2-the-untold-parts-of-using-any-6a328560048d