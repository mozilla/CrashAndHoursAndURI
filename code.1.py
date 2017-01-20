import datetime
import json
import random
import subprocess
import time
import pandas as pd
random.seed(10)
sampleids = [ random.randint(1,100) for x in range(2)]
samplechar = [ "'{}'".format(str(x)) for x in sampleids]
u0 = sqlContext.read.load("s3://telemetry-parquet/main_summary/v3", "parquet",mergeSchema=True)

u1 = u0.filter("app_name='Firefox' and normalized_channel='release' and os='Windows_NT' and sample_id in ({})".format( ",".join(samplechar)))
    
u2 = u1.select(u1.client_id,
               u1.sample_id, #remove when ready to do all counts
               u1.os,
               u1.app_version,
               u1.os_version,
               u1.crash_submit_success_main,
               u1.crashes_detected_content,
               u1.crashes_detected_gmplugin,
               u1.crashes_detected_plugin,
               u1.submission_date,
               u1.subsession_length,
               u1.total_uri_count,
               u1.unique_domains_count)
sqlContext.registerDataFrameAsTable(u2, "ms3")


ms4 = sqlContext.sql("""
select 
client_id,
app_version as version,
os_version as osversion,
submission_date as date,
sum(crashes_detected_content) as contentcrashes,
sum(crashes_detected_gmplugin) as  mediacrashes,
sum(crashes_detected_plugin) as plugincrashes,
sum(crash_submit_success_main) as  browsercrash,
sum(subsession_length)/3600 as  hrs,
sum(total_uri_count) as uri,
sum(unique_domains_count) as domain
from ms3
where submission_date>='20161201' and os_version in ('6.1', '6.2', '6.3', '10.0')
group by 1,2,3,4
""")

write = pyspark.sql.DataFrameWriter(ms4.coalesce(100))
write.csv(path="s3://mozilla-metrics/sguha/uricrashes/",mode='overwrite',nullValue="NA",header=True)



