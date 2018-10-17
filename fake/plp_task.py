import luigi
import datetime
from fake.fake_plp import FakePLP

from luigi import LocalTarget


class PLPTask(luigi.Task):
    input_dt = luigi.DateMinuteParameter(default=datetime.datetime.utcnow(), interval=5)

    def run(self):
        FakePLP(self.input_dt).run()

    def output(self):
        directory = "preprocessed/{date:year=%Y/month=%m/day=%d/hour=%H/minutes=%M}".format(date=self.input_dt)
        return LocalTarget(directory)
