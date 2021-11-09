import jpy

RuntimeMemory = jpy.get_type('io.deephaven.db.v2.utils.RuntimeMemory')
runtime_memory = RuntimeMemory.getInstance()
RuntimeMemorySample = jpy.get_type('io.deephaven.db.v2.utils.RuntimeMemory$Sample')

runtime_memory_pre = RuntimeMemorySample()
runtime_memory_pos = RuntimeMemorySample()
runtime_memory.read(runtime_memory_pre)

#
# Do a round of gc right before starting the test, to minimize the chances
# we require collections during the benchmark.
# Most relevant when a DH session is being reused for running several
# benchmarks in a row.
#
System = jpy.get_type('java.lang.System')
System.gc()
