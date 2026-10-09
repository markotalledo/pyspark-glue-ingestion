# Physical plan of the events output

```
== Physical Plan ==
AdaptiveSparkPlan (15)
+- Project (14)
   +- Filter (13)
      +- Window (12)
         +- WindowGroupLimit (11)
            +- Sort (10)
               +- Exchange (9)
                  +- WindowGroupLimit (8)
                     +- Sort (7)
                        +- Project (6)
                           +- Project (5)
                              +- Filter (4)
                                 +- InMemoryTableScan (1)
                                       +- InMemoryRelation (2)
                                             +- Scan json  (3)
```

What to read in it:

- **One shuffle, on `event_id`** (node 9, `Exchange`). That is the deduplication and the only wide operation in the job.
- **`WindowGroupLimit` on both sides of the shuffle** (nodes 8 and 11). Spark 3.5 sees `row_number() = 1` and drops extra copies inside each task before shuffling, so duplicates are mostly removed before they cross the network.
- **The date window is applied before the shuffle** (node 4). The code filters by `event_date` before deduplicating, and Spark rewrites it into a range on the parsed timestamp. Copies of an event share `occurred_at`, so filtering first gives the same result and shuffles only rows that will be kept.
- **Only the properties that are used get extracted** before the shuffle (`_extract_*` columns in node 9), not the whole nested struct.
- **`InMemoryTableScan`** is the cache on the raw read. Spark refuses queries that reference only the corrupt-record column of a JSON source without one, and the same raw frame feeds both the clean and the rejected outputs.

There is no skew to handle here: `event_id` is a UUID, so the shuffle spreads evenly. Skew shows up when you join or group on something like `customer_id` with a few very large customers; the fix would be salting that key or a broadcast join.
