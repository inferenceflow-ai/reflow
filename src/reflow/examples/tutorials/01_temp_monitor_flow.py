from typing import List

from cluster import FlowCluster

class RandomWalkTempSimulator:
    """
    Emulates a stream of randomly fluctuating temperature observations
    """
    def __init__(self, initial_temp: float, steps: List[float], max_count: int = 0):
        """

        Args:
            initial_temp:  the initial value of the temperature
            steps:         the possible increments (or decrements) to apply at each step - the value returned from
                           next() will be the present temp plus a randomly selected increment
            max_count:     the maximum number of values to produce - calling next after this will yield  an
                            StopIterationException
        """
        self.temp = initial_temp
        self.steps = steps
        self.count = 0

flow_cluster = FlowCluster(['tcp://192.168.1.101:5001'])
