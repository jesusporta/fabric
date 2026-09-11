# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************


# Tumbling window       Fixed non-overlapping time intervals. (duration 5 min) 
#                       Example: 10:00 - 10:05 - 10:10...
# Hopping window        Fixed overlapping time intervals. (duration 5 min, hope size 2 min) 
#                       Example: 10:00 - 10:05, 10:02 - 10:07...
# Sliding window        Triggered by incomming events {fixed duration + or - (backwards)}. (duration 5 min) 
#                       Example: event at 10:01 then 10:01 - 10:06, event at 10:03 then 10:03 - 10:08...
# Session window        Based on activity {start by event and lives as long events arriva up to max duration threshold (sessions, downtime)}
#                       (Max duration 10min, Timeout 5 sec). Open when event arrive, if next arrive 2 sec later, ok, but at second 7 stops if nothing new arrives
# Snapshot window       Groups events that arrives at the same time (no time configuration) (which orders were place at the same time? orders at the same time?)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
