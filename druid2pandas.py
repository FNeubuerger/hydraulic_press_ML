from pydruid.client import *
import matplotlib.pyplot as plt

query = PyDruid(druid_url_goes_here, 'druid/v2')

ts = query.timeseries(
    datasource='',
    granularity='',
    intervals='',
    aggregations={},
    post_aggregations={},
    filter=
)
df = query.export_pandas()
df['timestamp'] = df['timestamp'].map(lambda x: x.split('T')[0])

