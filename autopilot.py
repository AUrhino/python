# This is WIP.
# Placed here for others can work on it.

import os
import subprocess
# Check if something is UP or down based on the output of a command run.
# In this example, we are checking the output of the 'ip a' command and looking for a pattern to decide what to do next.

# Check Smarts  sm_service show | grep NOT RUNNING
# Check APG     manage-modules | grep STOPPED
# Check NCM     /bin/voyenced | grep not running


def main():
    print_header()
    check_smarts()
    check_apg()
    check_ncm()


def print_header():
    print('')
    print('----------------------------------------------------------')
    print('         AutoPilot                                        ')
    print(' Used to check things are runnning when your not watching ')
    print(' Add a file to /stop_autopilot.txt to pause fixing things ')
    print('----------------------------------------------------------')
    print('')

def check_stop():
#Use this file to "stop" the script without exiting. Since someone may forget to start it again.
fname = "/stop_autopilot.txt"
if os.path.isfile(fname):
    print("File does not exist. Checks can proceed.")


def check_smarts():
# Check 1
cmd1 = subprocess.check_output(["ip a show lo |grep state | awk '{ print $9 }'"], text=True,shell=True,encoding=None)
if 'UNKNOWN' in cmd1:
    print('UNKNOWN found. Do stuff')
elif 'DOWN' in cmd1:
	print('DOWN found'. Do stuff to fix)
	

if __name__ == '__main__':
    main()
