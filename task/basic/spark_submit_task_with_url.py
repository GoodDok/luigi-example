import logging
import subprocess
import tempfile

from luigi.contrib.external_program import ExternalProgramRunContext, ExternalProgramRunError
from luigi.contrib.spark import SparkSubmitTask

logger = logging.getLogger('luigi-interface')


class SparkSubmitTaskWithUrl(SparkSubmitTask):
    """
    Basic class for logging in spark submit applications
    """

    def run(self):
        """
        Overrides `run` of ExternalProgramTask, just gets URL from logs
        :return:
        """
        args = list(map(str, self.program_args()))

        logger.info('Running command: %s', ' '.join(args))
        env = self.program_environment()
        tmp_stdout, tmp_stderr = tempfile.TemporaryFile(), tempfile.TemporaryFile()
        proc = subprocess.Popen(args=args, env=env,
                                stdout=subprocess.PIPE, stderr=tmp_stderr, encoding='utf8')

        try:
            for line in proc.stdout:
                tmp_stdout.write(line.encode(encoding='utf8'))
                if "started at http://" in line:  # this line may differ
                    url_index = line.find('http://')
                    tracking_url = line[url_index:]
                    logger.info('Tracking url for the task is ' + tracking_url)
                    self.set_tracking_url(tracking_url)
                    tmp_stdout.write('==== See tracking url for further details ===='.encode(encoding='utf8'))
                    break

            with ExternalProgramRunContext(proc):
                proc.wait()
            success = proc.returncode == 0

            if not success:
                stdout = self._clean_output_file(tmp_stdout)
                stderr = self._clean_output_file(tmp_stderr)
                raise ExternalProgramRunError(
                    'Program failed with return code={}:'.format(proc.returncode),
                    args, env=env, stderr=stderr, stdout=stdout)
        finally:
            tmp_stderr.close()
            tmp_stdout.close()