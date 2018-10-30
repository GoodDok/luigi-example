import luigi

from task.basic.spark_submit_task_with_url import SparkSubmitTaskWithUrl


class PiTask(SparkSubmitTaskWithUrl):
    num_partitions = luigi.IntParameter(default=10000)

    def app_options(self):
        return [self.num_partitions]

    @property
    def py_files(self):
        return ['./pyspark/pi.py']



