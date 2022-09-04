#!/usr/bin/env python3
"""
"""

import csv
import sys
import datetime

class Instant:
    """A date and value"""

    def __init__(self, date, value):
        self.date = date
        self.value = value

    def __str__(self) -> str:
        return "%s at %s" % (self.value, self.date) 

class Period:
    """Class to store and analyse relevant information from maximum to maximum"""

    def __init__(self, maximum1, decline, minimum, maximum2):
        self.maximum1 = maximum1
        self.decline = decline
        self.minimum = minimum
        self.maximum2 = maximum2

    def __str__(self) -> str:
        info = "Initial maximum: " + str(self.maximum1) + "\n" \
            "Decline: " + str(self.decline) + "\n" \
            "Minimum: " + str(self.minimum) + "\n" \
            "Final maximum: " + str(self.maximum2)
        return info 

def main():
    pass

def test():
    instant = Instant("12.10.1985", 10000)
    period = Period(instant, instant, instant, instant)
    print(period)

if __name__ == '__main__' :
    test()