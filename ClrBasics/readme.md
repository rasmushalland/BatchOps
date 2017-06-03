# BatchOps

BatchOps: Batching database lookups in .NET using ```async```.

Accessing resources such as databases over the network usually is something that many or most applications do. However, with network access comes latency and reduced performance. For resources that are close (in terms of latency), that might not be a problem, until you need to access the resource many times, at which point the latencies become noticeable.

A common case of this problem is repeated lookups in a database. This problem can often be solved by doing batch loookups, but that might require restructuring the code significantly, harming readability of the code.

BatchOps provides another way of solving the problem, leveraging compiler support for
```async``` functions.

## Example

TODO

## Benchmarks

TODO

