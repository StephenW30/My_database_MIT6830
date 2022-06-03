# My_database_MIT6830

This is for myself to finish the curriculum: MIT 6.830. Using github in order to control different versions.

测试指令：

```shell
Lab1:
ant runtest -Dtest=TupleDescTest
ant runtest -Dtest=TupleTest
ant runtest -Dtest=CatalogTest
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapPageReadTest
ant runtest -Dtest=HeapFileReadTest
ant runsystest -Dtest=ScanTest

Lab2:
ant runtest -Dtest=PredicateTest
ant runtest -Dtest=JoinPredicateTest
ant runtest -Dtest=FilterTest
ant runtest -Dtest=JoinTest
ant runtest -Dtest=IntegerAggregatorTest
ant runtest -Dtest=StringAggregatorTest
ant runtest -Dtest=FilterTest
ant runtest -Dtest=AggregateTest
ant runsystest -Dtest=AggregateTest
ant runtest -Dtest=HeapPageWriteTest
ant runtest -Dtest=HeapFileWriteTest
ant runtest -Dtest=BufferPoolWriteTest
ant runtest -Dtest=InsertTest
ant runsystest -Dtest=InsertTest
ant runsystest -Dtest=DeleteTest
ant runsystest -Dtest=EvictionTest

Lab3
ant runtest -Dtest=IntHistogramTest
ant runtest -Dtest=TableStatsTest
ant runtest -Dtest=estimateJoinCostTest
ant runtest -Dtest=JoinOptimizerTest
ant runsystest -Dtest=QueryTest

Lab4
ant runtest -Dtest=LockingTest 
ant runtest -Dtest=TransactionTest
ant runsystest -Dtest=AbortEvictionTest
ant runtest -Dtest=DeadlockTest


Lab5

Lab6
```

Process recording：

    Up to now, several labs have been finished, including Lab1, Lab2 and Lab3. Those coding has passed the Unit test and System test provided for each exercise.

    tip:
    in Lab3, rewrite the iterator in HeapFile to accelerate the speed of searching so that it can pass the QueryTest, otherwise, error will occur.
