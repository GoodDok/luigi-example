import luigi
import datetime
from fake.plp_task import PLPTask
from fake.fake_td_aggregator import FakeTDAggr

from luigi import LocalTarget


class TDAggrTask(luigi.Task):
    input_dt = luigi.DateMinuteParameter(default=datetime.datetime.utcnow(), interval=10)

    def requires(self):
        return [PLPTask(date) for date in {self.input_dt, self.input_dt + datetime.timedelta(minutes=5)}]

    def run(self):
        FakeTDAggr(self.input_dt).run()

    def output(self):
        directory = "aggregated/{date:year=%Y/month=%m/day=%d/hour=%H/minutes=%M}".format(date=self.input_dt)
        return LocalTarget(directory)
