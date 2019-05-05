#coding=utf-8

class BackTesting(object):
    """description of class"""

    def __init__(
        self,Strategy,display
    ):
        self.BStrategy=Strategy
        self.display=display
    
    def do(self):
        self.BStrategy.Predictall()

        self.display.bar()
