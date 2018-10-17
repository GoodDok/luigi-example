#!/usr/bin/python
import datetime
import os
from datetime import datetime, timedelta


class FakeTDAggr(object):

    def __init__(self, input_dt):
        self.input_dt = input_dt  # type: datetime

    def run(self):
        if self.input_dt.minute % 10 != 0:
            raise ValueError('Input parameter\'s minutes should be a multiple of 10')

        input_directory00 = "preprocessed/{date:year=%Y/month=%m/day=%d/hour=%H/minutes=%M}".format(date=self.input_dt)
        input_directory05 = "preprocessed/{date:year=%Y/month=%m/day=%d/hour=%H/minutes=%M}".format(date=self.input_dt + timedelta(minutes=5))

        f00 = open(input_directory00 + "/output.csv", "r")
        f05 = open(input_directory05 + "/output.csv", "r")

        file00_contents = f00.read()
        file05_contents = f05.read()

        directory = "aggregated/{date:year=%Y/month=%m/day=%d/hour=%H/minutes=%M}".format(date=self.input_dt)
        if not os.path.exists(directory):
            os.makedirs(directory)

        f = open(directory + "/output.csv", "w+")
        f.write(file00_contents)
        f.write(file05_contents)
        f.close()
# luigi --module fake.td_aggr_task RangeByMinutes --of TDAggrTask --start 2018-10-09T0000 \
# --stop 2018-10-09T1200 --minutes-interval 10
