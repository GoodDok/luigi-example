import luigi


class TestSparkSubmitTask(SparkSubmitTask):
    deploy_mode = "client"
    name = "AppName"
    entry_class = "org.test.MyClass"
    jars = ["jars/my.jar"]
    py_files = ["file1.py", "file2.py"]
    files = ["file1", "file2"]
    conf = {"Prop": "Value"}
    properties_file = "conf/spark-defaults.conf"
    driver_memory = "4G"
    driver_java_options = "-Xopt"
    driver_library_path = "library/path"
    driver_class_path = "class/path"
    executor_memory = "8G"
    driver_cores = 8
    supervise = True
    total_executor_cores = 150
    executor_cores = 10
    queue = "queue"
    num_executors = 2
    archives = ["archive1", "archive2"]
    app = "file"

    def app_options(self):
        return ["arg1", "arg2"]

    def output(self):
        return luigi.LocalTarget('output')