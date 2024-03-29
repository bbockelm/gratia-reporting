'''
Author: Yaling Zheng
Jan 2012
Holland Computing Center, University of Nebraska-Lincoln 

This script produces a xrootd report in the following format within the last 24 hours
between UCSD local time yesterday 06:00:00 and today 06:00:00

All sites

Overflow: 314: (1.77% wall 0.83%) Normal:17435
Exit 0: 76.75% (vs 62.89%) wall 70.30%
Exit 84 or 85: 0.32% (vs 0.52%) wall 0.02%
Efficiency: 77.02% (vs 85.27%)
Eff  >80%: 62.74% (vs 54.38%)

Only UCSD+Nebraska+Wisconsin+Purdue

Overflow: 314: (1.78% wall 0.83%) Normal:17355
Exit 0: 76.75% (vs 62.74%) wall 70.30%
Exit 84 or 85: 0.32% (vs 0.52%) wall 0.02%
Efficiency: 77.02% (vs 85.27%)
Eff  >80%: 62.74% (vs 54.64%)

Possible Overflow Jobs with Exit Code 84 or 85 based on xrootd log

for cmssrv32.fnal.gov:1094:
    for /CN=Nicholas S Eggert 114717:
        408235.127, 2012-04-05 20:03:15 GMT--2012-04-05 20:13:20 GMT,
          /store/mc/Fall11/WJetsToLNu_TuneZ2_7TeV-madgraph-tauola/AODSIM/PU_S6_START42_V14B-v1/0000/1EEE763D-1AF2-E011-8355-00304867D446.root

'''

'''
For CMSSW_5_2_x, exit code 85 (8021 in the dashboard) indicates that the file open failed, the fallback was tried, and the fallback was tried, and the fallback also failed. Exit code 84 indicates the file open failed and no fallback was retried.
'''

import os
# from sets import Set
import re
import time
from time import gmtime, strftime
from datetime import datetime, date, timedelta;
from pytz import timezone
import MySQLdb
import ConfigParser
import string
# package of parsing options
import optparse
# package of sending emails
import smtplib
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart

os.environ['TZ'] = "US/Pacific"
time.tzset()
UCSDoffset = time.timezone/3600

today = datetime.today()
# NOTE - UCSD runs this on a 24-hour period, starting at 6am local.
# We must convert to a Unix epoch including DST, add the UCSD offset,
# then to a UTC datetime.
UCSD_start = datetime(today.year, today.month, today.day, 6, 0, 0)
UCSD_start_epoch = int(time.mktime(UCSD_start.timetuple())) + UCSDoffset/3600
LatestEndTime = datetime.utcfromtimestamp(UCSD_start_epoch)
EarliestEndTime = LatestEndTime - timedelta(1, 0)

os.environ['TZ'] = "US/Central"
time.tzset()
Nebraskaoffset = time.timezone/3600

xrootd_log_dir = "/var/log/xrootd"

# the message (which is in fact the xrootd report) to send to a group
# of people
outputmsg = ""

def parseArguments2():
    ReportDate = None
    ReportSender = None
    ReportReceiver = None
    JobEarliestEndTime = None
    JobLatestEndTime = None

    return ReportDate, ReportSender, ReportReceiver, JobEarliestEndTime, JobLatestEndTime

'''
Judge whether a date is valid
'''
def GetValidDate(date):
    if date:
        time_t = time.strptime(date, '%Y-%m-%d')
        return datetime(*time_t[:6])
    return None

g_Conn = None
def SetConnection(conn):
    """
    Set the module's connection object.
    """
    global g_Conn
    g_Conn = conn

'''
Connect to rcf-gratia.unl.edu, and prepare database gratia for querying
'''
def ConnectDatabase():
    if not g_Conn:
        raise Exception("Module connection has not been set; invoke overflow_jobs_report.SetConnection")

    cursor = g_Conn.cursor()
    
    # return database and cursor
    return cursor

'''
Query database gratia, and compute the following:
for all sites (and sites in the form of %UCSD% %Purdue% %Nebraska% %GLOW% (Grid Laboratory of Wisconsin))
(1) the number of overflow jobs, the percentage of number of overflow jobs OVER number of all jobs,
the percentage of walltime of overflow jobs OVER walltime of all jobs, the number of normal jobs
(2) the percentage of number of overflow jobs with exit code 0 OVER number of overflow jobs, 
the percentage of number of normal jobs with exit code 0 OVER number of normal jobs,
the percentage of walltime of overflow jobs with exit code 0 OVER walltime of overflow jobs 
(3) the percentage of number of overflow jobs with exit code 84 or 85 OVER number of overflow jobs, 
the percentage of number of normal jobs with exit code 84 or 85 OVER number of normal jobs,
the percentage of walltime of overflow jobs with exit code 84 or 85 OVER walltime of overflow jobs 
(4) the efficiency of overflow jobs, which is equal to (CpuUserDuration+CpuSystemDuration)/WallDuration,
the efficiency of normal jobs
(5) the percentage of number of overflow jobs whose efficiency is greater than 80%, 
the percentage of number of normal jobs whose efficiency is greater than 80%
'''

def QueryOverflowJobs(cursor):
    # The database keeps its begin/end times in UTC.
    global EarliestEndTime
    global LatestEndTime
    # Compute number (wallduration, CpuUserDuration+CpuSystemDuration) of overflow jobs in all sites
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone();
    NumOverflowJobs = int(row[0])
    if (NumOverflowJobs==0):
        WallDurationOverflowJobs = 0
        UserAndSystemDurationOverflowJobs = 0
    else:
        WallDurationOverflowJobs = float(row[1])
        UserAndSystemDurationOverflowJobs = float(row[2])
    # Compute efficiency of overflow jobs, which is equal to (CpuUserDuration+CpuSystemDuration)/WallDuration (in all sites)
    if (WallDurationOverflowJobs==0):
        EfficiencyOverflowJobs = 0
    else:
        EfficiencyOverflowJobs = float(100* UserAndSystemDurationOverflowJobs)/WallDurationOverflowJobs;

    return NumOverflowJobs, WallDurationOverflowJobs, UserAndSystemDurationOverflowJobs, EfficiencyOverflowJobs

def QueryNormalJobs(cursor, WallDurationOverflowJobs):
    # compute number (wallduration, CpuUserDuration+CpuSystemDuration) of normal jobs (in all sites)
    global EarliestEndTime
    global LatestEndTime
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone();
    NumNormalJobs = int(row[0])
    if (NumNormalJobs == 0):
        WallDurationNormalJobs = 0
        UserAndSystemDurationNormalJobs = 0
    else:
        WallDurationNormalJobs = float(row[1])  
        UserAndSystemDurationNormalJobs = float(row[2])
    WallDurationAllJobs = WallDurationOverflowJobs + WallDurationNormalJobs
    #print NumNormalJobs 

    # Compute the efficiency of normal jobs (in all sites)
    if (WallDurationNormalJobs == 0):
        EfficiencyNormalJobs = 0
    else:
        EfficiencyNormalJobs = float(100*UserAndSystemDurationNormalJobs)/WallDurationNormalJobs
    return NumNormalJobs, WallDurationNormalJobs, UserAndSystemDurationNormalJobs, EfficiencyNormalJobs

def QueryOverflowJobsExitCode0(cursor, NumOverflowJobs, WallDurationOverflowJobs):
    global EarliestEndTime
    global LatestEndTime
    # Compute the percentage of number (wallduration) of overflow jobs with exit code 0 OVER number of overflow jobs (in all sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND RESC.value = 0
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode0 = int(row[0])
    if (NumOverflowJobs==0):
        PercentageExitCode0Overflow = 0
    else:
        PercentageExitCode0Overflow =float(100*NumOverflowJobsExitCode0)/ NumOverflowJobs
    #print str(PercentageExitCode0Overflow) + "%"
    if (NumOverflowJobsExitCode0==0):
        WallDurationOverflowJobsExitCode0 = 0
    else:
        WallDurationOverflowJobsExitCode0 = float(row[1]) 

    # Compute the walltime percentage of walltime of overflow jobs with exit code 0 OVER walltime of overflow jobs (in all sites)
    if (WallDurationOverflowJobs==0):
        PercentageWallDurationOverflowJobsExitCode0 = 0
    else:
        PercentageWallDurationOverflowJobsExitCode0 = float(100*WallDurationOverflowJobsExitCode0)/WallDurationOverflowJobs;
    return NumOverflowJobsExitCode0, WallDurationOverflowJobsExitCode0, PercentageExitCode0Overflow, PercentageWallDurationOverflowJobsExitCode0

def QueryNormalJobsExitCode0(cursor, NumNormalJobs): 
    global EarliestEndTime
    global LatestEndTime
    # Compute the percentage of number normal jobs with exit code 0 OVER number of normal jobs (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND RESC.value = 0
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode0 = int(row[0])
    if (NumNormalJobs==0):
        PercentageExitCode0Normal = 0
    else:
        PercentageExitCode0Normal = float(100*NumNormalJobsExitCode0)/NumNormalJobs
    #print str(PercentageExitCode0Normal) + "%"
    return NumNormalJobsExitCode0, PercentageExitCode0Normal

def QueryOverflowJobsExitCode84or85(cursor, NumOverflowJobs, WallDurationOverflowJobs):
    global EarliestEndTime
    global LatestEndTime
    # compute number of overflow jobs with exit code 84 or 85 (in all sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND (RESC.value = 84 or RESC.value = 85)
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode84or85 = int(row[0])

    # Compute the percentage of number of overflow jobs with exit code 84 or 85 OVER number of overflow jobs (in all sites)
    if (NumOverflowJobs==0):
        PercentageNumOverflowJobsExitCode84or85 = 0
    else:
        PercentageNumOverflowJobsExitCode84or85 = float(100*NumOverflowJobsExitCode84or85)/NumOverflowJobs;
    #print str(PercentageNumOverflowJobsExitCode84or85)+"%"

    # compute walltime of overflow jobs with exit code 84 (in all sites)
    if (NumOverflowJobsExitCode84or85==0):
        WallDurationOverflowJobsExitCode84or85 = 0
    else:
        WallDurationOverflowJobsExitCode84or85 = float(row[1])
    #print str(WallDurationOverflowJobsExitCode84or85)

    if (WallDurationOverflowJobs==0):
        PercentageWallDurationOverflowJobsExitCode84or85 = 0
    else:
        # Compute the percentage of walltime of overflow jobs with exit code 84 or 85 OVER walltime of overflow jobs (in all sites) 
        PercentageWallDurationOverflowJobsExitCode84or85 = float(100*WallDurationOverflowJobsExitCode84or85)/WallDurationOverflowJobs;     

    #print str(PercentageWallDurationOverflowJobsExitCode84or85)+"%"
    return NumOverflowJobsExitCode84or85, WallDurationOverflowJobsExitCode84or85, PercentageNumOverflowJobsExitCode84or85, PercentageWallDurationOverflowJobsExitCode84or85

def QueryNormalJobsExitCode84or85(cursor, NumNormalJobs):
    global EarliestEndTime
    global LatestEndTime
    # compute number of normal jobs with exit code 84 or 85 (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND (RESC.value = 84 or RESC.value = 85)
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode84or85 = int(row[0])

    # Compute the percentage of number of normal jobs with exit code 84 or 85 OVER number of normal jobs (in all sites) 
    if (NumNormalJobs==0):
        PercentageNumNormalJobsExitCode84or85 = 0
    else:
        PercentageNumNormalJobsExitCode84or85 = float(100*NumNormalJobsExitCode84or85)/NumNormalJobs
    #print str(PercentageNumNormalJobsExitCode84or85)+"%"
    return NumNormalJobsExitCode84or85, PercentageNumNormalJobsExitCode84or85

def QueryOverflowJobsEfficiencyGT80(cursor, NumOverflowJobs):
    global EarliestEndTime
    global LatestEndTime
    # Compute the percentage of number of overflow jobs whose efficiency greater than 80% (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentOverflowJobs = int(row[0])
    if (NumOverflowJobs == 0):
        PercentageEfficiencyGT80percentOverflowJobs = 0
    else:
        PercentageEfficiencyGT80percentOverflowJobs = float(100*NumEfficiencyGT80percentOverflowJobs)/NumOverflowJobs
    #print str(PercentageEfficiencyGT80percentOverflowJobs) + "%"
    return NumEfficiencyGT80percentOverflowJobs, PercentageEfficiencyGT80percentOverflowJobs

def QueryNormalJobsEfficiencyGT80(cursor, NumNormalJobs):
    global EarliestEndTime
    global LatestEndTime
    # Compute the percentage of number of normal jobs whose efficiency greater than 80% (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR
    join JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentNormalJobs = int(row[0])
    if (NumNormalJobs == 0):
        PercentageEfficiencyGT80percentNormalJobs = 0
    else:
        PercentageEfficiencyGT80percentNormalJobs = float(100*NumEfficiencyGT80percentNormalJobs)/NumNormalJobs
    #print str(PercentageEfficiencyGT80percentNormalJobs) + "%"
    return NumEfficiencyGT80percentNormalJobs, PercentageEfficiencyGT80percentNormalJobs

def QueryJobs4sites(cursor):
    global EarliestEndTime
    global LatestEndTime
    # Compute the number (walltime) of all jobs in 4 sites
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumAllJobs4sites = int(row[0])
    if (NumAllJobs4sites==0):
        WallDurationAllJobs4sites = 0
    else:
        WallDurationAllJobs4sites = float(row[1])
    return NumAllJobs4sites, WallDurationAllJobs4sites

def QueryOverflowJobs4sites(cursor, WallDurationAllJobs4sites):
    global EarliestEndTime
    global LatestEndTime
    # Compute the number of overflow jobs that in %UCSD%, %Nebraska%, %GLOW%, and %Purdue%
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobs4sites = int(row[0])
    if (NumOverflowJobs4sites==0):
        WallDurationOverflowJobs4sites = 0
        UserAndSystemDurationOverflowJobs4sites = 0
    else:
        WallDurationOverflowJobs4sites = float(row[1])
        UserAndSystemDurationOverflowJobs4sites = float(row[2])
    # Compute the percentage of walltime of overflow jobs OVER walltime of all jobs (in 4 sites)
    if (WallDurationAllJobs4sites == 0):
        PercentageWallDurationOverflowJobs4sites = 0
    else:
        PercentageWallDurationOverflowJobs4sites = float(100*WallDurationOverflowJobs4sites)/WallDurationAllJobs4sites
    #print str(PercentageWallDurationOverflowJobs4sites)+"%"
    # Compute the efficiency of overflow jobs (in 4 sites)
    if (WallDurationOverflowJobs4sites == 0):
        EfficiencyOverflowJobs4sites = 0
    else:
        EfficiencyOverflowJobs4sites = float(100* UserAndSystemDurationOverflowJobs4sites)/WallDurationOverflowJobs4sites
    return NumOverflowJobs4sites, WallDurationOverflowJobs4sites, UserAndSystemDurationOverflowJobs4sites,PercentageWallDurationOverflowJobs4sites, EfficiencyOverflowJobs4sites
    #print str(NumOverflowJobs4sites)

def QueryNormalJobs4sites(cursor):
    global EarliestEndTime
    global LatestEndTime
    # Compute the number (efficiency) of normal jobs (in 4 sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobs4sites = int(row[0])
    #print str(NumNormalJobs4sites)
    if (NumNormalJobs4sites == 0):
        UserAndSystemDurationNormalJobs4sites = 0
        WallDurationNormalJobs4sites = 0
    else:
        UserAndSystemDurationNormalJobs4sites = float(row[2])
        WallDurationNormalJobs4sites = float(row[1])
    if (WallDurationNormalJobs4sites == 0):
        EfficiencyNormalJobs4sites = 0
    else:
        EfficiencyNormalJobs4sites = float(100*UserAndSystemDurationNormalJobs4sites)/WallDurationNormalJobs4sites
    #print str(EfficiencyNormalJobs4sites) + "%"
    return NumNormalJobs4sites, WallDurationNormalJobs4sites, UserAndSystemDurationNormalJobs4sites, EfficiencyNormalJobs4sites

def QueryOverflowJobsExitCode0foursites(cursor, NumOverflowJobs4sites, WallDurationOverflowJobs4sites):
    global EarliestEndTime
    global LatestEndTime
    # Compute the number of overflow jobs with exit code 0 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND RESC.value = 0
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode0foursites = int(row[0])
    # Compute the percentage of number of overflow jobs with exit code 0 OVER number of overflow jobs (in 4 sites)
    if (NumOverflowJobs4sites == 0):
        PercentageOverflowJobsExitCode0foursites = 0
    else:
        PercentageOverflowJobsExitCode0foursites = float(100*NumOverflowJobsExitCode0foursites)/NumOverflowJobs4sites
    #print str(PercentageOverflowJobsExitCode0foursites)+"%"
    if (NumOverflowJobsExitCode0foursites == 0):
        WallDurationOverflowJobsExitCode0foursites = 0
    else:
        WallDurationOverflowJobsExitCode0foursites = int(row[1])
    # Compute the percentage of walltime of overflow jobs with exit code 0 OVERFLOW the walltime of all overflow jobs (in 4 sites)
    if (WallDurationOverflowJobs4sites == 0):
        PercentageWallDurationOverflowJobsExitCode0foursites = 0
    else:
        PercentageWallDurationOverflowJobsExitCode0foursites = float(100*WallDurationOverflowJobsExitCode0foursites)/WallDurationOverflowJobs4sites
    return NumOverflowJobs4sites, WallDurationOverflowJobsExitCode0foursites, PercentageOverflowJobsExitCode0foursites, PercentageWallDurationOverflowJobsExitCode0foursites

def QueryNormalJobsExitCode0foursites(cursor, NumNormalJobs4sites):
    global EarliestEndTime
    global LatestEndTime
    # Compute the number of normal jobs with exit code 0 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND RESC.value = 0
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode0foursites = int(row[0])
    # Compute the percentage of number of normal jobs with exit code 0 OVER number of normal jobs (in 4 sites)
    if (NumNormalJobs4sites == 0):
        PercentageNormalJobsExitCode0foursites = 0
    else:
        PercentageNormalJobsExitCode0foursites = float(100*NumNormalJobsExitCode0foursites)/NumNormalJobs4sites
    #print str(PercentageNormalJobsExitCode0foursites)+"%"
    return NumNormalJobsExitCode0foursites, PercentageNormalJobsExitCode0foursites

def QueryOverflowJobsExitCode84or85foursites(cursor, NumOverflowJobs4sites, WallDurationOverflowJobs4sites):
    global EarliestEndTime
    global LatestEndTime
    # Compute number of overflow jobs with exit code 84 or 85 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND (RESC.value = 84 or RESC.value = 85)
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode84or85foursites = int(row[0])
    # Compute percentage of number of overflow jobs with exit code 84 or 85 OVER number of overflow jobs (in 4 sites)
    if (NumOverflowJobs4sites == 0):
        PercentageOverflowJobsExitCode84or85foursites = 0
    else:
        PercentageOverflowJobsExitCode84or85foursites = float(100*NumOverflowJobsExitCode84or85foursites)/NumOverflowJobs4sites
    #print str(PercentageOverflowJobsExitCode84or85foursites)+"%"
    if (NumOverflowJobsExitCode84or85foursites == 0):
        WallDurationOverflowJobsExitCode84or85foursites = 0
    else:
        WallDurationOverflowJobsExitCode84or85foursites = float(row[1])
    # Compute the percentage of walltime of overflow jobs with exit code 84 or 85 OVER walltime of overflow jobs (in 4 sites) 
    if (WallDurationOverflowJobs4sites == 0):
        PercentageWallDurationOverflowJobsExitCode84or85foursites  = 0
    else:
        PercentageWallDurationOverflowJobsExitCode84or85foursites = float(100*WallDurationOverflowJobsExitCode84or85foursites)/WallDurationOverflowJobs4sites
    #print str(PercentageWallDurationOverflowJobsExitCode84or85foursites)+"%"
    return NumOverflowJobsExitCode84or85foursites, WallDurationOverflowJobsExitCode84or85foursites, PercentageOverflowJobsExitCode84or85foursites, PercentageWallDurationOverflowJobsExitCode84or85foursites 

def QueryNormalJobsExitCode84or85foursites(cursor, NumNormalJobs4sites): 
    global EarliestEndTime
    global LatestEndTime
    # Compute number of normal jobs with exit code 84 or 85 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND (RESC.value = 84 or RESC.value = 85)
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode84or85foursites = int(row[0])
    # Compute percentage of number of normal jobs with exit code 84 or 85 OVER number of normal jobs (in 4 sites)
    if (NumNormalJobs4sites == 0):
        PercentageNormalJobsExitCode84or85foursites = 0
    else:
        PercentageNormalJobsExitCode84or85foursites = float(100*NumNormalJobsExitCode84or85foursites)/NumNormalJobs4sites
    #print str(PercentageNormalJobsExitCode84or85foursites)+"%"
    return NumNormalJobsExitCode84or85foursites, PercentageNormalJobsExitCode84or85foursites

def QueryOverflowJobsEfficiencyGT80foursites(cursor, NumOverflowJobs4sites):
    global EarliestEndTime
    global LatestEndTime
    # Compute the percentage of number of overflow jobs whose efficiency greater than 80% (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration>0.8
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentOverflowJobs4sites = int(row[0])
    if (NumOverflowJobs4sites == 0):
        PercentageEfficiencyGT80percentOverflowJobs4sites = 0
    else:
        PercentageEfficiencyGT80percentOverflowJobs4sites = float(100*NumEfficiencyGT80percentOverflowJobs4sites)/NumOverflowJobs4sites
    #print str(PercentageEfficiencyGT80percentOverflowJobs4sites) + "%"
    return NumEfficiencyGT80percentOverflowJobs4sites, PercentageEfficiencyGT80percentOverflowJobs4sites

def QueryNormalJobsGT80percent4sites(cursor, NumNormalJobs4sites):
    global EarliestEndTime
    global LatestEndTime
    # Compute the percentage of normal jobs whose efficiency greater than 80% (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>=%s and EndTime<%s
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (HostDescription like '%%Nebraska%%' or HostDescription like '%%UCSD%%' or HostDescription like '%%Purdue%%' or HostDescription like '%%GLOW%%')
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration>0.8
      AND JURM.ProbeName="condor:glidein-2.t2.ucsd.edu"
     """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentNormalJobs4sites = int(row[0])
    if (NumNormalJobs4sites == 0):
        PercentageEfficiencyGT80percentNormalJobs4sites = 0
    else:
        PercentageEfficiencyGT80percentNormalJobs4sites = float(100*NumEfficiencyGT80percentNormalJobs4sites)/NumNormalJobs4sites
    #print str(PercentageEfficiencyGT80percentNormalJobs4sites) + "%"
    return NumEfficiencyGT80percentNormalJobs4sites, PercentageEfficiencyGT80percentNormalJobs4sites    

def QueryGratia(cursor):
    QueryGratiaJobsAllSites(cursor)
    QueryGratiaJobs4sites(cursor)

def QueryGratiaJobsAllSites(cursor):
    # The database keeps its begin/end times in UTC.
    (NumOverflowJobs, WallDurationOverflowJobs, UserAndSystemDurationOverflowJobs, EfficiencyOverflowJobs) = QueryOverflowJobs(cursor)
    (NumNormalJobs, WallDurationNormalJobs, UserAndSystemDurationNormalJobs, EfficiencyNormalJobs) = QueryNormalJobs(cursor, WallDurationOverflowJobs)
    NumAllJobs = NumOverflowJobs + NumNormalJobs
    WallDurationAllJobs = WallDurationOverflowJobs + WallDurationNormalJobs
    # Compute the percentage of number of overflow jobs OVER number of all jobs (in all sites)
    if (NumAllJobs==0):
        PercentageOverflowJobs = 0
    else:
        PercentageOverflowJobs = float(NumOverflowJobs*100)/NumAllJobs;
    # Compute the percentage of walltime of overflow jobs OVER walltime of all jobs (in all sites)
    if (WallDurationAllJobs==0):
        PercentageWallDurationOverflowJobs = 0
    else:
        PercentageWallDurationOverflowJobs = float(WallDurationOverflowJobs*100)/WallDurationAllJobs;
    (NumOverflowJobsExitCode0, WallDurationOverflowJobsExitCode0, PercentageExitCode0Overflow, PercentageWallDurationOverflowJobsExitCode0) = QueryOverflowJobsExitCode0(cursor, NumOverflowJobs, WallDurationOverflowJobs)
    (NumNormalJobsExitCode0, PercentageExitCode0Normal) = QueryNormalJobsExitCode0(cursor, NumNormalJobs)
    (NumOverflowJobsExitCode84or85, WallDurationOverflowJobsExitCode84or85, PercentageNumOverflowJobsExitCode84or85, PercentageWallDurationOverflowJobsExitCode84or85) = QueryOverflowJobsExitCode84or85(cursor, NumOverflowJobs, WallDurationOverflowJobs)
    (NumNormalJobsExitCode84or85, PercentageNumNormalJobsExitCode84or85) = QueryNormalJobsExitCode84or85(cursor, NumNormalJobs)
    (NumEfficiencyGT80percentoverflowJobs, PercentageEfficiencyGT80percentOverflowJobs) = QueryOverflowJobsEfficiencyGT80(cursor, NumOverflowJobs)
    (NumEfficiencyGT80percentNormalJobs, PercentageEfficiencyGT80percentNormalJobs) = QueryNormalJobsEfficiencyGT80(cursor, NumNormalJobs)
    PrintStatisticsBasedOnQueryGratiaJobsAllSites(NumOverflowJobs, PercentageOverflowJobs, PercentageWallDurationOverflowJobs, NumNormalJobs, PercentageExitCode0Overflow, PercentageExitCode0Normal, PercentageWallDurationOverflowJobsExitCode0, PercentageNumOverflowJobsExitCode84or85, PercentageNumNormalJobsExitCode84or85, PercentageWallDurationOverflowJobsExitCode84or85, EfficiencyOverflowJobs, EfficiencyNormalJobs, PercentageEfficiencyGT80percentOverflowJobs, PercentageEfficiencyGT80percentNormalJobs)

def QueryGratiaJobs4sites(cursor):
    (NumAllJobs4sites, WallDurationAllJobs4sites) = QueryJobs4sites(cursor)
    (NumOverflowJobs4sites, WallDurationOverflowJobs4sites, UserAndSystemDurationOverflowJobs4sites,PercentageWallDurationOverflowJobs4sites, EfficiencyOverflowJobs4sites) = QueryOverflowJobs4sites(cursor, WallDurationAllJobs4sites)
    (NumNormalJobs4sites, WallDurationNormalJobs4sites, UserAndSystemDurationNormalJobs4sites, EfficiencyNormalJobs4sites) = QueryNormalJobs4sites(cursor)
    # Compute the percentage of number of overflow jobs OVER number of all jobs (in 4 sites)
    if (NumAllJobs4sites == 0):
        PercentageOverflowJobs4sites = 0
    else:
        PercentageOverflowJobs4sites = float(100*NumOverflowJobs4sites)/NumAllJobs4sites;
    (NumOverflowJobs4sites, WallDurationOverflowJobsExitCode0foursites, PercentageOverflowJobsExitCode0foursites, PercentageWallDurationOverflowJobsExitCode0foursites) = QueryOverflowJobsExitCode0foursites(cursor, NumOverflowJobs4sites, WallDurationOverflowJobs4sites)
    (NumNormalJobExitCode0foursites, PercentageNormalJobsExitCode0foursites) = QueryNormalJobsExitCode0foursites(cursor, NumNormalJobs4sites)
    (NumOverflowJobsExitCode84or85foursites, WallDurationOverflowJobsExitCode84or85foursites, PercentageOverflowJobsExitCode84or85foursites, PercentageWallDurationOverflowJobsExitCode84or85foursites) = QueryOverflowJobsExitCode84or85foursites(cursor, NumOverflowJobs4sites, WallDurationOverflowJobs4sites) 
    (NumNormalJobsExitCode84or85foursites, PercentageNormalJobsExitCode84or85foursites) = QueryNormalJobsExitCode84or85foursites(cursor, NumNormalJobs4sites)
    (NumEfficiencyGT80percentOverflowJobs4sites, PercentageEfficiencyGT80percentOverflowJobs4sites) = QueryOverflowJobsEfficiencyGT80foursites(cursor, NumOverflowJobs4sites)
    (NumEfficiencyGT80percentNormalJobs4sites, PercentageEfficiencyGT80percentNormalJobs4sites) = QueryNormalJobsGT80percent4sites(cursor, NumNormalJobs4sites)
    PrintStatisticsBasedOnQueryGratiaJobs4sites(NumOverflowJobs4sites, PercentageOverflowJobs4sites, PercentageWallDurationOverflowJobs4sites, NumNormalJobs4sites, PercentageOverflowJobsExitCode0foursites, PercentageNormalJobsExitCode0foursites, PercentageWallDurationOverflowJobsExitCode0foursites, PercentageOverflowJobsExitCode84or85foursites, PercentageWallDurationOverflowJobsExitCode84or85foursites, PercentageNormalJobsExitCode84or85foursites, EfficiencyOverflowJobs4sites, EfficiencyNormalJobs4sites, PercentageEfficiencyGT80percentOverflowJobs4sites, PercentageEfficiencyGT80percentNormalJobs4sites)

def PrintStatisticsBasedOnQueryGratiaJobsAllSites(NumOverflowJobs, PercentageOverflowJobs, PercentageWallDurationOverflowJobs, NumNormalJobs, PercentageExitCode0Overflow, PercentageExitCode0Normal, PercentageWallDurationOverflowJobsExitCode0, PercentageNumOverflowJobsExitCode84or85, PercentageNumNormalJobsExitCode84or85, PercentageWallDurationOverflowJobsExitCode84or85, EfficiencyOverflowJobs, EfficiencyNormalJobs, PercentageEfficiencyGT80percentOverflowJobs, PercentageEfficiencyGT80percentNormalJobs):    
    # Print out the statistics 
    msg= "\nAll sites\n"
    print msg
    global outputmsg
    outputmsg = msg
    msg =  "%15s %d (%5.2f%s wall %5.2f%s) %15s%d" % ("Overflow:", NumOverflowJobs, PercentageOverflowJobs, "%", PercentageWallDurationOverflowJobs, "%", "Normal:", NumNormalJobs)
    print msg
    msg += "\n"
    outputmsg += msg
    msg =  "%15s %5.2f%s (vs %5.2f%s) wall %5.2f%s" % ("Exit 0:", PercentageExitCode0Overflow, "%", PercentageExitCode0Normal, "%", PercentageWallDurationOverflowJobsExitCode0, "%")
    print msg
    msg += "\n"
    outputmsg += msg
    msg =  "%15s %5.2f%s (vs %5.2f%s) wall %5.2f%s" % ("Exit 84 or 85:", PercentageNumOverflowJobsExitCode84or85, "%", PercentageNumNormalJobsExitCode84or85,"%", PercentageWallDurationOverflowJobsExitCode84or85, "%")
    print msg
    msg += "\n"
    outputmsg += msg
    msg =  "%15s %5.2f%s (vs %5.2f%s)" % ("Efficiency:", EfficiencyOverflowJobs, "%", EfficiencyNormalJobs, "%")
    print msg
    msg += "\n"
    outputmsg += msg
    msg =  "%15s %5.2f%s (vs %5.2f%s)" % ("Eff >80%:", PercentageEfficiencyGT80percentOverflowJobs, "%", PercentageEfficiencyGT80percentNormalJobs, "%")
    print msg
    msg += "\n"
    outputmsg += msg

def PrintStatisticsBasedOnQueryGratiaJobs4sites(NumOverflowJobs4sites, PercentageOverflowJobs4sites, PercentageWallDurationOverflowJobs4sites, NumNormalJobs4sites, PercentageOverflowJobsExitCode0foursites, PercentageNormalJobsExitCode0foursites, PercentageWallDurationOverflowJobsExitCode0foursites, PercentageOverflowJobsExitCode84or85foursites, PercentageWallDurationOverflowJobsExitCode84or85foursites, PercentageNormalJobsExitCode84or85foursites, EfficiencyOverflowJobs4sites, EfficiencyNormalJobs4sites, PercentageEfficiencyGT80percentOverflowJobs4sites, PercentageEfficiencyGT80percentNormalJobs4sites):
    global outputmsg
    msg = "\nOnly UCSD+Nebraska+Wisconsin+Purdue\n"
    print msg
    msg += "\n"
    outputmsg += msg
    msg = "%15s %d (%5.2f%s wall %5.2f%s) %15s%d" % ("Overflow:", NumOverflowJobs4sites, PercentageOverflowJobs4sites, "%", PercentageWallDurationOverflowJobs4sites, "%", "Normal:", NumNormalJobs4sites)
    print msg
    msg += "\n"
    outputmsg += msg
    msg = "%15s %5.2f%s (vs %5.2f%s) wall %5.2f%s" % ("Exit 0:", PercentageOverflowJobsExitCode0foursites, "%", PercentageNormalJobsExitCode0foursites, "%", PercentageWallDurationOverflowJobsExitCode0foursites, "%") 
    print msg
    msg += "\n"
    outputmsg += msg
    msg = "%15s %5.2f%s (vs %5.2f%s) wall %5.2f%s" % ("Exit 84 or 85:", PercentageOverflowJobsExitCode84or85foursites, "%", PercentageNormalJobsExitCode84or85foursites, "%", PercentageWallDurationOverflowJobsExitCode84or85foursites, "%")
    print msg
    msg += "\n"
    outputmsg += msg
    msg =  "%15s %5.2f%s (vs %5.2f%s)" % ("Efficiency:", EfficiencyOverflowJobs4sites, "%", EfficiencyNormalJobs4sites, "%")
    print msg
    msg+= "\n"
    outputmsg += msg
    msg =  "%15s %5.2f%s (vs %5.2f%s)" % ("Eff >80%:", PercentageEfficiencyGT80percentOverflowJobs4sites, "%", PercentageEfficiencyGT80percentNormalJobs4sites, "%")
    print msg
    msg += "\n"
    outputmsg += msg

'''
For each overflow job with exit code 84 or 85, we check possible correponding job in xrootd log
and output the job in the following format
for cmssrv32.fnal.gov:1094:
    for /CN=Nicholas S Eggert 114717:
      408235.127, 2012-04-05 20:03:15 GMT--2012-04-05 20:13:20 GMT,
       /store/mc/Fall11/WJetsToLNu_TuneZ2_7TeV-madgraph-tauola/AODSIM/PU_S6_START42_V14B-v1/0000/1EEE763D-1AF2-E011-8355-00304867D446.root
'''
def FilterCondorJobsExitCode84or85(cursor):
    # Find those overflow jobs whose exit code is 84 or 85 and resource type is BatchPilot 
    querystring = """
    SELECT JUR.dbid, LocalJobId, CommonName, Host, StartTime, EndTime, AI.Value
    from JobUsageRecord JUR
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    LEFT OUTER JOIN Resource AI on ((AI.dbid = JUR.dbid) and (AI.description="AppInfo"))
    JOIN JobUsageRecord_Meta JURM on JUR.dbid = JURM.dbid
    where 
      EndTime >= %s AND EndTime < %s
      AND ResourceType = "BatchPilot"
      AND (RESC.value = 84 or RESC.value = 85)
      AND HostDescription like '%%-overflow';
    """
    cursor.execute(querystring, (EarliestEndTime, LatestEndTime));
    # Handle each record
    numrows = int(cursor.rowcount)
    for i in range(numrows):
        row = cursor.fetchone()
        #print row[0], row[1], row[2], row[3], row[4], row[5]
        localjobid = row[1]
        commonname = row[2]
        host = row[3]
        starttime = row[4]
        endtime = row[5]
        applicationname = row[6]
	gmstarttime = starttime
	gmendtime = endtime
	#print host
	if (host!="NULL"):
            # Check each job in xrootd log
            matchedflag = CheckJobMatchInXrootdLog_ExactMatch(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime, applicationname)
            if (not matchedflag):
                #print 'yes, not match\n'
                CheckJobMatchInXrootdLog_FuzzyMatch(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime, applicationname)

'''
for the parameters,
localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime
are all a job's information read from rcf-gratia database.

Below is the format of analysis of possible overflow xrootd jobs with exitcode 84 or 85 in the following format
for cmssrv32.fnal.gov (a redirection site)
   for user .../.../CN=Brian (a x509UserProxyVOName)
     jobid matched-to-jobids-gratia 
     locajobid starttime endtime jobfilename 

Brian and I guess the overflow jobs as follows. First, we search the
gratia database the overflow jobs with exit code 84 or 85. Then, for each
such job J, we refer the xrootd.unl.edu log file, and find
corresponding xrootd.unl.edu records by guessing: if xrootd log show
that there is a job whose host machine matches this job's host
machine, and whose login time is within 10 minutes of job J's start
time and whose disconnection time is within 10 minutes of job J's
disconnection time, then this job is a possible xrootd overflow job.

We check our corresponding xrootd log, and see whether we can track
the activity of this job.  We check 2 dictionaries:
jobLoginDisconnectionAndSoOnDictionary and hostnameJobsDictionary, and
see whether there exist jobs that satisfying the the requirement
'''
def CheckJobMatchInXrootdLog_ExactMatch(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime, applicationname):
    # host from rcf-gratia
    hostnameitems = host.split(" ")
    # hostname from rcf-gratia
    hostname = hostnameitems[0]
    # print hostname
    possiblejobs = hostnameJobsDictionary.get(hostname, None)
    if (not possiblejobs):
        # we try to find the abbreviation of the hostname, 
        # for example red-mon.unl.edu, its abbreviation is red-mon
        hostnameitems = hostname.split(".")
        abbHostname = hostnameitems[0]
        possiblejobs = hostnameJobsDictionary.get(abbHostname, None)
    # print possiblejobs
    # mktime assumes local time, but starttime is in UTC.
    # Use "utctimetuple" to force it to a timestamp *forcing no DST*, and
    # subtract off the timezone.  This procedure is DST-safe.
    jobBeginAt = int(time.mktime(starttime.utctimetuple())) - Nebraskaoffset * 3600
    jobEndAt = int(time.mktime(endtime.utctimetuple())) - Nebraskaoffset * 3600
    flag = None
    time_now = time.time()
    if possiblejobs:
        for job in possiblejobs:
            #print job
            LoginDisconnectionTimeAndSoOn = jobLoginDisconnectionAndSoOnDictionary[job]
            retrieved_loginTime = LoginDisconnectionTimeAndSoOn[1]
            retrieved_disconnectionTime = LoginDisconnectionTimeAndSoOn[2]
            if (not retrieved_loginTime):
                loginTime = 0
            else:
                loginTime = int(retrieved_loginTime)
            if (not retrieved_disconnectionTime):
                disconnectionTime = time_now + 100
            else:
                disconnectionTime = int(retrieved_disconnectionTime)
            if ((loginTime >= jobBeginAt+0) and (loginTime <= jobBeginAt + 600)):
                if ((jobEndAt >= disconnectionTime+0) and (jobEndAt <= disconnectionTime + 600)):
                    # Note that, the dictionary jobLoginDisconnectionAndSoOnDictionary needs to be updated
                    # jobid -- corresponded to rcf-gratia job ids, Login time, disconnectiontime, filename, redirectiontime
                    LoginDisconnectionTimeAndSoOn[0] = set([hostname])
                    jobLoginDisconnectionAndSoOnDictionary[job] = LoginDisconnectionTimeAndSoOn
        	    retrieved_filename = LoginDisconnectionTimeAndSoOn[3]
                    if (not retrieved_filename):
                        retrieved_filename = ""
                    retrieved_redirectionsite = LoginDisconnectionTimeAndSoOn[4]
                    if (not retrieved_redirectionsite):
                        retrieved_redirectionsite = "Jobs not redirected to any site"
                    if redirectionsite_vs_users_dictionary.get(retrieved_redirectionsite, None):
                        redirectionsite_vs_users_dictionary[retrieved_redirectionsite].add(commonname)
                    else:
                        redirectionsite_vs_users_dictionary[retrieved_redirectionsite]=set([commonname])
                    # we need also update another
                    key_of_redirectionsiteuser = retrieved_redirectionsite + "."+ commonname
		    str_gmstarttime = gmstarttime.strftime("%Y-%m-%d %H:%M:%S GMT")
	   	    str_gmendtime = gmendtime.strftime("%Y-%m-%d %H:%M:%S GMT")
                    if applicationname is None:
                        strapplicationname = ""
                    else:
                        strapplicationname = applicationname+", \n"
                    if (not redirectionsiteuser_vs_jobs_dictionary.get(key_of_redirectionsiteuser, None)):
                        redirectionsiteuser_vs_jobs_dictionary[key_of_redirectionsiteuser] = set([job + "(XROOTD hostname)," + ConvertSetToString(LoginDisconnectionTimeAndSoOn[0])+"(GRATIA hostname) \n        "+ localjobid+" ,"+ str_gmstarttime + "--" + str_gmendtime + ", \n          " + strapplicationname + retrieved_filename])
                    else:
                        redirectionsiteuser_vs_jobs_dictionary[key_of_redirectionsiteuser].add(job + "(XROOTD hostname)," + ConvertSetToString(LoginDisconnectionTimeAndSoOn[0]) + "(GRATIA hostname), \n        "+ localjobid+", "+str_gmstarttime + "--" + str_gmendtime + ", \n          " + strapplicationname + retrieved_filename)
		    #for key, value in redirectionsite_vs_users_dictionary.iteritems():
		    #	print key
		    #    print value
		    #for key, value in redirectionsiteuser_vs_jobs_dictionary.iteritems():
		    #	print key
		    #	print value			                        
                    flag = 1
    return flag

# If two host names are valid host names,
# for example,
# xrootdhostname in the form of  osg_cmsu.23360:113@g22n16.hep.wisc.edu
# gratiahostname in the form of rossmann-a207.rcac.purdue.edu 
# The last part of rcac.purdue.edu has to be matched.
# 
'''
Brian: A Purdue-specific rule is that, when jobs go through the NAT,
the hostname is of the form "nat*". So, this is a possible match:
  
cmsprod7.21374:2658@nat129.rcac.purdue.edu(GRATIA hostname),
rossmann-a097.rcac.purdue.edu (XROOTD hostname)
  
This is not a possible match:
  
cmsprod7.21962:2632@cms-147.rcac.purdue.edu(GRATIA hostname),
rossmann-a097.rcac.purdue.edu (XROOTD hostname)
'''
def ARE_MATCHED_HOSTNAMES(xrootdhostname, gratiahostname):
    #print "xrootdhostname gratiahostname"
    #print xrootdhostname
    #print gratiahostname
    matchedflag = 1
    if Is_a_valid_hostname(xrootdhostname) and Is_a_valid_hostname(gratiahostname):
        pos = gratiahostname.find(".")
        lattergratiahostname = gratiahostname[pos:]
        if xrootdhostname.find(lattergratiahostname)<0:
            matchedflag = None
        if xrootdhostname.find(".purdue.")>=0:
            if xrootdhostname.find("@nat")<0:
                matchedflag=None
    #print "match?"
    #print matchedflag
    return matchedflag

# It is a valid host name if it includes meaning domain name which
# includes .edu, .gov, .org, .com
def Is_a_valid_hostname(hostname1):
    if hostname1.find(".edu")>=0 or hostname1.find(".gov")>=0 or hostname1.find(".org") or hostname1.find(".com"):
        return 1
    return None

'''
Below is the format of analysis of possible overflow xrootd jobs with exitcode 84 or 85 in the following format
for cmssrv32.fnal.gov (a redirection site)
   for user .../.../CN=Brian (a x509UserProxyVOName)
      locajobid starttime endtime jobfilename 

Brian and I guess the overflow jobs as follows. First, we search the
gratia database the overflow jobs with exit code 84 or 85. Then, for each
such job J, we refer the xrootd.unl.edu log file, and find
corresponding xrootd.unl.edu records by guessing: if xrootd log show
that there is a job whose valid domain name (.org, .com, .edu, .gov)
matches J's valid domain name (when one of the domain name is not
valid, we assume they are a possible match; purdue university's xrootd
host name has to include @nat) and whose login time is within 10
minutes of job J's start time and whose disconnection time is within
10 minutes of job J's disconnection time, then this job is a possible
xrootd overflow job.

We check our corresponding xrootd log, and see whether we can track
the activity of this job.  We check 1 dictionary:
jobLoginDisconnectionAndSoOnDictionary to see whether there exist jobs
that satisfying the the requirement

When there are multiple FUZZY MATCHES, we choose not to print it out

'''
def CheckJobMatchInXrootdLog_FuzzyMatch(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime, applicationname):
    # host from rcf-gratia
    hostnameitems = host.split(" ")
    # hostname from rcf-gratia
    hostname = hostnameitems[0]
    # mktime assumes local time, but starttime is in UTC.
    # Use "utctimetuple" to force it to a timestamp *forcing no DST*, and
    # subtract off the timezone.  This procedure is DST-safe.
    jobBeginAt = int(time.mktime(starttime.utctimetuple())) - Nebraskaoffset * 3600
    jobEndAt = int(time.mktime(endtime.utctimetuple())) - Nebraskaoffset * 3600
    flag = None
    time_now = time.time()
    NUMBER_OF_FUZZY_MATCHES = 0
    foundjob = None
    for key,value in jobLoginDisconnectionAndSoOnDictionary.iteritems():
        job = key
        # judge hostname (GRATIA hostname) and job (xrootd hostname) match or not
        if ARE_MATCHED_HOSTNAMES(job, hostname):
            #print job
            #print "matched!!!!!!"
            LoginDisconnectionTimeAndSoOn = jobLoginDisconnectionAndSoOnDictionary[job]
            retrieved_loginTime = LoginDisconnectionTimeAndSoOn[1]
            retrieved_disconnectionTime = LoginDisconnectionTimeAndSoOn[2]
            if (not retrieved_loginTime):
                loginTime = 0
            else:
                loginTime = int(retrieved_loginTime)
            if (not retrieved_disconnectionTime):
                disconnectionTime = time_now + 100
            else:
                disconnectionTime = int(retrieved_disconnectionTime)
            if ((loginTime >= jobBeginAt+0) and (loginTime <= jobBeginAt + 600)):
                if ((jobEndAt >= disconnectionTime+0) and (jobEndAt <= disconnectionTime + 600)):
                    #print localjobid, commonname, host, starttime, endtime
                    # we did not find a hostname in xrootd correspond the one in rcf-gratia,
                    # so we try to guess maybe the hostname in xrootd and rcf-gratia are different.
                    # therefore, the first item in the jobLoginDisconnectionAndSoOnDictionary 
                    # records this information (correspond to the jobid in rcf-gratia)
                    #print "yes, found one"
                    NUMBER_OF_FUZZY_MATCHES += 1
                    if (NUMBER_OF_FUZZY_MATCHES == 1):
                        foundjob = job
    if (NUMBER_OF_FUZZY_MATCHES == 1):
        founduniquejobLoginDisconnectionTimeAndSoOn = jobLoginDisconnectionAndSoOnDictionary[foundjob]
        founduniquejobLoginDisconnectionTimeAndSoOn[0] = set([hostname])
        foundjob_filename = founduniquejobLoginDisconnectionTimeAndSoOn[3]
        foundjob_redirectionsite = founduniquejobLoginDisconnectionTimeAndSoOn[4]
        if (not foundjob_filename):
            foundjob_filename = ""
        if (not foundjob_redirectionsite):
            foundjob_redirectionsite = "Jobs not redirected to any site"
        # build the redirectionsite_vs_users_dictionary for output
        if redirectionsite_vs_users_dictionary.get(foundjob_redirectionsite, None):
            redirectionsite_vs_users_dictionary[foundjob_redirectionsite].add(commonname)
        else:
            redirectionsite_vs_users_dictionary[foundjob_redirectionsite]=set([commonname])
        # we need also update another
        key_of_redirectionsiteuser = foundjob_redirectionsite + "."+ commonname
        str_gmstarttime = gmstarttime.strftime("%Y-%m-%d %H:%M:%S GMT")
        str_gmendtime = gmendtime.strftime("%Y-%m-%d %H:%M:%S GMT")
        foundjob_gratiahostname = founduniquejobLoginDisconnectionTimeAndSoOn[0]
        jobLoginDisconnectionAndSoOnDictionary[job] = founduniquejobLoginDisconnectionTimeAndSoOn
        if applicationname is None:
            strapplicationname = ""
        else:
            strapplicationname = applicationname + ",\n"
        if (not redirectionsiteuser_vs_jobs_dictionary.get(key_of_redirectionsiteuser, None)):
            redirectionsiteuser_vs_jobs_dictionary[key_of_redirectionsiteuser] = set([foundjob+"(XROOTD hostname), "+ ConvertSetToString(foundjob_gratiahostname)+"(GRATIA hostname), \n        "+ localjobid+", "+str_gmstarttime + "--" + str_gmendtime + ", \n          " + strapplicationname + foundjob_filename])
        else:
            redirectionsiteuser_vs_jobs_dictionary[key_of_redirectionsiteuser].add(foundjob+"(XROOTD hostname), "+ ConvertSetToString(foundjob_gratiahostname)+"(GRATIA hostname), \n        " + localjobid+", "+str_gmstarttime + "--" + str_gmendtime + ", \n          " + strapplicationname + foundjob_filename )
    return NUMBER_OF_FUZZY_MATCHES


'''
Covert a set to a printable string
'''
def ConvertSetToString(aset):
    resultstr = ""
    for oneitem in aset:
        resultstr = resultstr + str(oneitem)+ " "
    return resultstr

'''                    
 the following two hash tables are defined so that we can output the following content easier:
 for cmssrv32.fnal.gov (a redirection site)
   for user /....../CN=Brian (a x509UserProxyVOName)
   jobid, correspond-job-ids-in-gratia    
   1234.0, 8:00-12:00, /store/foo
'''
redirectionsite_vs_users_dictionary = {}
redirectionsiteuser_vs_jobs_dictionary = {}

jobLoginDisconnectionAndSoOnDictionary = {}
hostnameJobsDictionary  = {}

# File name in the remove list will not built into the dictionary when scanning xrootd log
FilenameRemoveList = ["/store/test/xrootd"]

# Host name in the remove list will not built into the dictionary when scanning xrootd log
HostnameRemoveList = ["red-mon"]

def Is_Jobid_in_HostnameRemoveList(jobid):
    inremovelist = None
    for oneitem in HostnameRemoveList:
        if jobid.find(oneitem)>=0:
            inremovelist=1
            break
    return inremovelist

'''
build a dictonary in such a format by scanning the xrootd logs
key     value
jobid   [matched-to-jobids-on-gratia, login time, disconnection time, filename, redirection site]

Note that matched-to-jobids-on-gratia are a set of hostnames that it matches to.
Sometimes a matched-to-jobid-on-gratia and jobid are not exactly same, however, it could be possible they
are in fact the same hostname. 
We list both of them so that the report reader can judge. 
'''
def buildJobLoginDisconnectionAndSoOnDictionary(filename):
    infile = open(filename)
    # we scan this line
    while 1:
        line = infile.readline()
        if not line:
            break
        # judge whether it is a login record 
        matchflagLogin = re.match("(\d{2})(\d{2})(\d{2}) (\d{2}:\d{2}:\d{2}) \d+ XrootdXeq: (\S+) login\s*", line, 0)
        if matchflagLogin:
            # we try to build a dictionary
            TheLoginDatetime = "20"+matchflagLogin.group(1)+"-"+matchflagLogin.group(2)+"-"+matchflagLogin.group(3)+" "+matchflagLogin.group(4); 
            logintimestamp =  int(time.mktime(time.strptime(TheLoginDatetime, '%Y-%m-%d %H:%M:%S')))
            jobid = matchflagLogin.group(5)
            if (not Is_Jobid_in_HostnameRemoveList(jobid)):
                curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                if (not curjobLoginDisconnectionAndSoOn):
                    curjobLoginDisconnectionAndSoOn = [None, None, None, None, None]
                    # includes matched-to-jobid-on-gratia, login time, disconnection time, filename, and redirection site
                curjobLoginDisconnectionAndSoOn[1] = logintimestamp
                jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
                # jobid from xrootd log, in the form of nagois.17030:522@red-mon
                jobiditems = jobid.split("@")
                # currenthostname in the form of red-mon
                currenthostname = jobiditems[1]
                # this is a full host name
                # from the dictionary, get several jobs whose name matches red-mon
                currentjobs = hostnameJobsDictionary.get(currenthostname, None)
                if (not currentjobs):
                    currentjobs=[]
                # jobid from xrootd log
                currentjobs.append(jobid)
                # update hostnameJobsDictionary
                hostnameJobsDictionary[currenthostname] = currentjobs
                #print currenthostname
                #print hostnameJobsDictionary[currenthostname]
        else: # else we judge whether it is a disconnection record
            matchflagDisconnection = re.match("(\d{2})(\d{2})(\d{2}) (\d{1,2}:\d{2}:\d{2}) \d+ XrootdXeq: (\S+) disc \d{1,2}:\d{2}:\d{2}\n", line)
            if matchflagDisconnection:
                # we try to 
                TheDisconnectionDatetime = "20"+matchflagDisconnection.group(1)+"-"+matchflagDisconnection.group(2)+"-"+matchflagDisconnection.group(3)+" "+matchflagDisconnection.group(4); 
                disconnectiontimestamp =  int(time.mktime(time.strptime(TheDisconnectionDatetime, '%Y-%m-%d %H:%M:%S')))
                jobid = matchflagDisconnection.group(5)
                if (not Is_Jobid_in_HostnameRemoveList(jobid)):
                    curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                    if (not curjobLoginDisconnectionAndSoOn):
                        curjobLoginDisconnectionAndSoOn = [None, None, None, None, None]
                    curjobLoginDisconnectionAndSoOn[2] = disconnectiontimestamp
                    jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
            else:
                matchflagFilenameRedirectionsite = re.match("\d{6} \d{1,2}:\d{2}:\d{2} \d+ Decode xrootd redirects (\S+) to (\S+) (\S+)\n", line)
                if matchflagFilenameRedirectionsite:
                    # we try to
                    jobid = matchflagFilenameRedirectionsite.group(1)
                    redirectionsite = matchflagFilenameRedirectionsite.group(2)
                    thisjobfilename = matchflagFilenameRedirectionsite.group(3)
                    if (not Is_Jobid_in_HostnameRemoveList(jobid)) and (not Is_Filename_In_RemoveList(thisjobfilename)):
                        curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                        if (not curjobLoginDisconnectionAndSoOn):
                            curjobLoginDisconnectionAndSoOn = [None, None, None, None, None]
                        curjobLoginDisconnectionAndSoOn[3] = thisjobfilename
                        curjobLoginDisconnectionAndSoOn[4] = redirectionsite
                        jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
                else:
                    # 120514 11:01:47 29552 osg_cmsu.21234:3383@g19n27.hep.wisc.edu ofs_open: 0-644 fn=/store/user/spadhi/DoublePartonWWFastSim_CMSSW425PUv1/DoublePartonWWFastSim_CMSSW425PUv1/82f55a4338de93f4ae1b4d1c54da954e/reco_34_1_Nzg.root
                    matchflagPureFilename = re.match("\d{6} \d{1,2}:\d{2}:\d{2} \d+ (\S+) ofs_open: \d+-\d+ fn=(\S+)\n", line)
                    if matchflagPureFilename:
                    # we try to
                        jobid = matchflagPureFilename.group(1)
                        thisjobfilename = matchflagPureFilename.group(2)
                        if (not Is_Jobid_in_HostnameRemoveList(jobid)) and (not Is_Filename_In_RemoveList(thisjobfilename)):
                            curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                            if (not curjobLoginDisconnectionAndSoOn):
                                curjobLoginDisconnectionAndSoOn = [None, None, None, None, None]
                            # if this job id not yet has a file name (especially get information from previous REDIRECTS pattern
                            if (not curjobLoginDisconnectionAndSoOn[3]):
                                curjobLoginDisconnectionAndSoOn[3] = thisjobfilename
                                jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn

                    
    infile.close()
    # now, we show the dictonary
    #for key,value in jobLoginDisconnectionAndSoOnDictionary.iteritems():
     #  print key
      # print value
    #for key, value in hostnameJobsDictionary.iteritems():
    # print key
    # print value

def Is_Filename_In_RemoveList(filename):
    inremovelist = None
    for oneremoveitem in FilenameRemoveList:
        if filename.find(oneremoveitem)>=0:
            inremovelist = 1
            break;
    return inremovelist


'''
output result in the following format
for cmssrv32.fnal.gov (a redirection site)
   for user /....../CN=Brian (a x509UserProxyVOName)
       xrootd host name, gratia host name
       1234.0, 8:00-12:00, /store/foo
'''
def PrintPossibleOverflowJobs():
    global redirectionsite_vs_users_dictionary
    # which are redirectionsite, x509UserProxyVOName, 
    # xrootd host name, gratia host name, 
    # localjobid, job start time GMT, job end date GMT, redirection file name
    overflowoutputmsg =  "\nPossible Overflow Jobs with Exit Code 84 or 85 based on xrootd log"
    for key,value in redirectionsite_vs_users_dictionary.iteritems():
        # if the key(redirection site) is 'None', we print out 
        # "Jobs not redirected to any site"
        msg =  "\nfor "+ key+":"
        print msg
        msg = msg + "\n"
        overflowoutputmsg += msg
        for oneuser in set(value):
            msg = "    for "+ oneuser+":"
            print msg
            msg +="\n"
            overflowoutputmsg += msg
            cur_key_value = key + "."+oneuser
            for onejob in redirectionsiteuser_vs_jobs_dictionary[cur_key_value]:
                msg =  "        "+onejob
                print msg
                msg += "\n"
                overflowoutputmsg += msg
    print overflowoutputmsg
    return overflowoutputmsg

def mainGetOverflowjobsInfo1():
    global EarliestEndTime
    global LatestEndTime
    global outputmsg
    (ReportDate, ReportSender, ReportReceiver, JobEarliestEndTime, JobLatestEndTime)= parseArguments2()
    ReportDateString = today.strftime("%Y-%m-%d")
    if ReportDate!=None:
        #print str(ReportDate)
        ReportDateString = str(ReportDate)
        ReportDay = time.strptime(str(ReportDate), "%Y-%m-%d 00:00:00")
        UCSD_start = datetime(ReportDay.tm_year, ReportDay.tm_mon, ReportDay.tm_mday, 6, 0, 0)
        UCSD_start_epoch = int(time.mktime(UCSD_start.timetuple())) + UCSDoffset/3600
        LatestEndTime = datetime.utcfromtimestamp(UCSD_start_epoch)
        EarliestEndTime = LatestEndTime - timedelta(1, 0)
    if JobEarliestEndTime!= None:
        EarliestEndTime = JobEarliestEndTime
    if JobLatestEndTime != None:
        LatestEndTime =  JobLatestEndTime
    # connect the database server rcf-gratia.unl.edu
    cursor = ConnectDatabase()
    if cursor:
        # query database gratia, and output statistic results
        QueryGratia(cursor)
    return outputmsg

def mainGetOverflowjobsInfo2():
    global EarliestEndTime
    global LatestEndTime
    (ReportDate, ReportSender, ReportReceiver, JobEarliestEndTime, JobLatestEndTime)= parseArguments2()
    ReportDateString = today.strftime("%Y-%m-%d")
    if ReportDate!=None:
        #print str(ReportDate)
        ReportDateString = str(ReportDate)
        ReportDay = time.strptime(str(ReportDate), "%Y-%m-%d 00:00:00")
        UCSD_start = datetime(ReportDay.tm_year, ReportDay.tm_mon, ReportDay.tm_mday, 6, 0, 0)
        UCSD_start_epoch = int(time.mktime(UCSD_start.timetuple())) + UCSDoffset/3600
        LatestEndTime = datetime.utcfromtimestamp(UCSD_start_epoch)
        EarliestEndTime = LatestEndTime - timedelta(1, 0)
    if JobEarliestEndTime!= None:
        EarliestEndTime = JobEarliestEndTime
    if JobLatestEndTime != None:
        LatestEndTime =  JobLatestEndTime
    # connect the database server rcf-gratia.unl.edu
    cursor = ConnectDatabase()
    returntext = ""
    if cursor:
        # Get all the filenames in the form of xrootd.log, then for each file, build the hash table
        filenames = os.listdir(xrootd_log_dir)
        for filename in filenames:
            if filename.startswith("xrootd.log"):
                buildJobLoginDisconnectionAndSoOnDictionary(os.path.join(xrootd_log_dir, filename))
        # check with xrootd log, and output possible overflow jobs with exit code 84 or 85
        FilterCondorJobsExitCode84or85(cursor)
        returntext = PrintPossibleOverflowJobs()
    return returntext

