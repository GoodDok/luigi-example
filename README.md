# luigi-example

To work with this example:

* install luigi
* start `luigid`
* run something like this:
```
export PYTHONPATH='.' && luigi --module fake.td_aggr_task RangeByMinutes --of TDAggrTask --start 2018-10-09T0000 \
--stop 2018-10-09T1200 --minutes-interval 10
```
