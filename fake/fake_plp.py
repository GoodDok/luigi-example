#!/usr/bin/python
import datetime
import os


class FakePLP(object):

    def __init__(self, input_dt):
        self.input_dt = input_dt

    def run(self):
        if self.input_dt.minute % 5 != 0:
            raise ValueError('Input parameter\'s minutes should be a multiple of 5')

        directory = "preprocessed/{date:year=%Y/month=%m/day=%d/hour=%H/minutes=%M}".format(date=self.input_dt)
        if not os.path.exists(directory):
            os.makedirs(directory)

        f = open(directory + "/output.csv", "w+")
        for i in range(10):
            f.write("{date:%Y-%m-%d %H:%M:%S}\n"
                    .format(date=(datetime.datetime.utcnow() + datetime.timedelta(minutes=i))))
        f.close()
