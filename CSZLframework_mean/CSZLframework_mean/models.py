#coding=utf-8
import abc



class BaseModel(object):
    """description of class"""

    def __init__(self):
        pass

    #@abc.abstractmethod
    def predict(self, dataset):
        """Subclass must implement this."""
        return 1

class LGBmodel(BaseModel):


    def predict(self, dataset):
        """Subclass must implement this."""
        return 2
