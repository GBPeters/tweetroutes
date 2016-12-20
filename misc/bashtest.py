__author__ = 'gijspeters'

from subprocess import call


while call("jython -J-Xmx5G -J-XX:-UseGCOverheadLimit /Users/gijspeters/Documents/Ontwikkelen/python/tweetroutes/otprun.py", shell=True)!= 0:
    continue