import luigi

from task.basic.spark_submit_task_with_url import SparkSubmitTaskWithUrl


class PiTask(SparkSubmitTaskWithUrl):
    num_partitions = luigi.IntParameter(default=100)
    app = 'pyspark_example/pi.py'

    def app_options(self):
        return [self.num_partitions]



