from __future__ import print_function

import os
import sys
import uuid
import time
import socket
import signal
import getpass
from threading import Thread
from os.path import abspath, join, dirname

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict

TASK_CPU = 0.5
TASK_MEM = 100
#EXECUTOR_CPUS = 1
#EXECUTOR_MEM = 100
TERMINAL_STATES = ["TASK_FINISHED","TASK_FAILED","TASK_KILLED","TASK_ERROR","TASK_LOST"]
DOCKER_TASK= 'cirobarradov/executor-app'

class MinimalScheduler(Scheduler):

    def __init__(self):
        #self.executor = executor
        self._message="localhost sample keyRegresion"

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        for offer in offers:
            try:
                cpus = self.getResource(offer.resources, 'cpus')
                mem = self.getResource(offer.resources, 'mem')
                if cpus < TASK_CPU or mem < TASK_MEM:
                    continue

                task = Dict()
                task_id = str(uuid.uuid4())
                task.task_id.value = task_id
                task.agent_id.value = offer.agent_id.value
                task.name = 'task {}'.format(task_id)
                #task.executor= self.executor
                task.container.type = 'DOCKER'
                task.container.docker.image = DOCKER_TASK #os.getenv('DOCKER_TASK')
                task.container.docker.network = 'HOST'
                task.container.docker.force_pull_image = True

                task.command.shell = True
                task.command.value = '/app/task.sh ' + self._message+str(uuid.uuid4())
                # task.container.type = 'DOCKER'
                # task.container.docker.image = DOCKER_TASK #os.getenv('DOCKER_TASK')
                # task.container.docker.network = 'HOST'
                # task.container.docker.force_pull_image = True

                task.resources = [
                    dict(name='cpus', type='SCALAR', scalar={'value': TASK_CPU}),
                    dict(name='mem', type='SCALAR', scalar={'value': TASK_MEM}),
                ]

                driver.launchTasks(offer.id, [task], filters)
            except Exception as e:
                logging.info(str(e))
                driver.declineOffer(offer.id, filters)
            pass
    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def statusUpdate(self, driver, update):
        logging.info(update)
        if (update.state in TERMINAL_STATES):
            if update.state == 'TASK_FAILED':
                logging.info('Failed')
                logging.info(update.message)
            elif update.state == 'TASK_ERROR':
                logging.info('Error')
                logging.info(update.message)
        else :
            logging.info("not finished")
        logging.debug('Status update TID %s %s',
                      update.task_id.value,
                      update.state)


def main(master):
    # executor = Dict()
    # executor.executor_id.value = 'MinimalExecutor'
    # executor.name = executor.executor_id.value
    # executor.command.value = '%s %s' % (
    #     sys.executable,
    #     abspath(join(dirname(__file__), 'executor.py'))
    # )
    # executor.resources = [
    #     dict(name='mem', type='SCALAR', scalar={'value': EXECUTOR_MEM}),
    #     dict(name='cpus', type='SCALAR', scalar={'value': EXECUTOR_CPUS}),
    # ]

    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = "MinimalFramework"
    framework.hostname = socket.gethostname()

    driver = MesosSchedulerDriver(
        MinimalScheduler(),
        framework,
        master,
        use_addict=True,
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('Scheduler running, Ctrl+C to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
