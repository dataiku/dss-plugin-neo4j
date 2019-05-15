from py2neo import Graph
from dataiku.runnables import Runnable

class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.config = config
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        raise Exception("unimplemented")
        