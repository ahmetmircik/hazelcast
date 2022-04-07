# Should have
- Feature should be optional 
- Same guarantees as in now. All read and write operations must be covered. 
- Scheduling latency may exceed map-store transaction latency.
  This lowers this feature's usefulness.
- During offloading, all add, remove, get can overlap with each other in any order, 
  offloading should preserve current behavior on same key.
- Key can be locked before map-store ops start?


# Implementation
- Put all LOAD or STORE operations to a queue 
- Do no other execution and off-load LOAD/STORE to another thread
- Exit from the current operation execution
- When LOAD/STORE finishes, re-submit map operation with the associated results 
- And finish execution of map operation
- How to handle post-processing?
= How to handle exceptions during offload?
- Register/de-register map operation

# Questions
- How will it behave during partition migrations?