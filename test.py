import signal
import time
import os

def handle_signal(signum, frame):
    raise IOError(signum,os.strerror(signum))

signal.signal(signal.SIGHUP, handle_signal)
signal.signal(signal.SIGUSR1, handle_signal)

signal_names = [name for name in dir(signal)
                if name.startswith("SIG") and not name.startswith("SIG_")]
signal_dict = {}
for name in signal_names:
    signal_dict[getattr(signal,name)] = name

while True:
    try:
        time.sleep(1)
    except IOError as e:
        print(e.errno)
        print(signal_dict[e.errno])


