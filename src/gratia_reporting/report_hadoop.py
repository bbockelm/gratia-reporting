#!/usr/bin/python2.4
 
import sys
import math
import sets
import time
import types
import socket
import datetime
import xml.dom.minidom

import gratia_reporting.make_table as make_table

SER_query = """
SELECT
  SE.UniqueID, SE.ParentID, SE.Name, SE.SpaceType, SE.Implementation, SE.Version,
  SER.MeasurementType, SER.TotalSpace, SER.FreeSpace, SER.UsedSpace, SER.FileCount, SER.FileCountLimit, SE.Status
FROM
  (SELECT * from (SELECT
     SER.Timestamp as Timestamp, SER.UniqueID as UniqueID, SER.MeasurementType as MeasurementType, TotalSpace, FreeSpace, UsedSpace, FileCount, FileCountLimit
   FROM StorageElementRecord SER FORCE INDEX(Timestamp)
   JOIN (SELECT MAX(Timestamp) as Timestamp, UniqueID, MeasurementType FROM StorageElementRecord FORCE INDEX(Timestamp) WHERE Timestamp >= %s and Timestamp <= %s GROUP BY UniqueID, MeasurementType) as foo ON foo.UniqueID=SER.UniqueID AND foo.MeasurementType=SER.MeasurementType AND foo.Timestamp = SER.Timestamp
   WHERE
     SER.Timestamp >= %s and SER.Timestamp <= %s
   GROUP BY SER.UniqueID, SER.MeasurementType
  ) as SER2 GROUP BY UniqueID, MeasurementType) as SER
JOIN (
  SELECT * from StorageElement SE1
  JOIN (
    SELECT UniqueID as UID, MAX(Timestamp) as MTM
    FROM StorageElement
    WHERE Timestamp <= %s and SE=%s
    GROUP BY UniqueID
  ) as SE2
    ON SE2.MTM=SE1.Timestamp and SE2.UID=SE1.UniqueID
  WHERE Timestamp <= %s AND SE=%s GROUP BY UniqueID
) as SE on SE.UniqueID=SER.UniqueID
"""

parents_query = """
SELECT
  SE.UniqueID,
  SE.ParentID,
  SE.Name,
  SE.SpaceType,
  SE.Version,
  SE.SiteName,
  SE.Implementation,
  SE.Status
FROM 
  (SELECT
     dbid,
     UniqueID,
     COALESCE(Timestamp, '1970-01-01 00:00:00') as time
   FROM StorageElement force index (Timestamp)
   WHERE
     (Timestamp <= %%s OR Timestamp IS NULL)
     AND UniqueID in (%s)
   GROUP BY UniqueID
  ) AS SE2
  JOIN StorageElement SE on SE2.dbid=SE.dbid
"""

cmds_query = """
select
  JobName as Name,
  ProjectName as SEUniqueID,
  R.Value as TagName,
  JURX.ExtraXml as Xml,
  EndTime
FROM JobUsageRecord JUR
JOIN Resource R on R.dbid=JUR.dbid and R.Description="CustomInfo"
JOIN JobUsageRecord_Xml JURX on JURX.dbid = JUR.dbid
WHERE
  EndTime >= %s AND EndTime <= %s
GROUP BY Name, SEUniqueID
HAVING MAX(EndTime)
"""

def GB(bytes):
    try:
        return int(round(float(bytes)/1000.**3))
    except:
        return 'UNKNOWN'

def avg(sizes):
    if not sizes:
        return "UNKNOWN"
    return sum(sizes) / float(len(sizes))

def stddev(sizes):
    if not sizes:
        return "UNKNOWN"
    my_avg = avg(sizes)
    sum_dev = sum([(i-my_avg)**2 for i in sizes])
    return math.sqrt(sum_dev/float(len(sizes)))

class SEInfo(object):

    def __init__(self, se_name, date, conn, log):
        self._db = conn
        self._date = date
        self._se_name = se_name
        self._log = log
        self.sas = {}
        self._custom = {}
        self.query()
        self.query_parents()
        self.query_cmds()

    def _execute(self, stmt, *args):
        if isinstance(args, types.ListType) or isinstance(args,
                types.TupleType):
            args2 = tuple(['"%s"' % i for i in args])
            self._log.info(stmt % args2)
        elif isinstance(args, types.DictType):
            args2 = dict([(i, '"%s"' % j) for (i, j) in args.items()])
            self._log.info(stmt % args2)
        else:
            self._log.info(stmt % args)
        timer = -time.time()
        curs = self._db.cursor()
        curs.execute(stmt, args)
        timer += time.time()
        self._log.info("Query took %.2f seconds." % timer)
        return curs
    
    def query(self):
        date = self._date.strftime('%Y-%m-%d')
        start_date = date + " 00:00:00"
        end_date   = date + " 23:59:59"
        self.results = self._execute(SER_query, start_date, end_date,
            start_date, end_date, end_date, self._se_name, end_date,
            self._se_name).fetchall()
        for result in self.results:
            uniqId, parentId, name, spaceType, implementation, version, \
                measurementType, totalSpace, freeSpace, usedSpace, fileCount, \
                fileCountLimit, status = result
            info = {'UniqueID': uniqId, 'ParentID': parentId, 'Name': name,
                'SpaceType': spaceType, 'Implementation': implementation,
                'Version': version, 'MeasurementType': measurementType,
                'TotalSpace': totalSpace, 'FreeSpace': freeSpace, 'FileCount':\
                fileCount, 'FileCountLimit': fileCountLimit, 'UsedSpace': \
                usedSpace, 'Status': status}
            if info['MeasurementType'] == 'logical' and info['SpaceType'] == 'SE':
                continue
            self.sas[info['UniqueID']] = info

    def query_parents(self):
        needed_parents = []
        for area in self.sas.values():
            if area['ParentID'] not in self.sas and area['ParentID'] not in \
                    needed_parents:
                needed_parents.append(area['ParentID'])
        if not needed_parents:
            return
        list_expr = ', '.join(('%s',)*len(needed_parents))
        my_parents_query = parents_query % list_expr
        self.results = self._execute(my_parents_query, self._date.strftime( \
            '%Y-%m-%d') + " 23:59:59", *needed_parents).fetchall()
        for result in self.results:
            uniqID, parentID, name, spaceType, version, siteName, impl, \
                status = result
            info = {'UniqueID': uniqID, 'ParentID': parentID, 'Name': name,
                'SpaceType': spaceType, 'Version': version, 'Status': status}
            self.sas[info['UniqueID']] = info

    def query_cmds(self):
        date = self._date.strftime("%Y-%m-%d")
        start_date = date + " 00:00:00"
        end_date   = date + " 23:59:59"
        results = self._execute(cmds_query, start_date, end_date)
        for result in results:
            name, unique_id, tag_name, xml_str, _ = result
            se_cmds = self._custom.setdefault(unique_id, {})
            try:
                dom = xml.dom.minidom.parseString(xml_str).\
                    getElementsByTagName(tag_name)[0]
                output = str(dom.firstChild.data)
            except:
                continue
            se_cmds[name] = output

    def SEs(self):
        result = [i for i in self.sas.values() if i['SpaceType'] == 'SE' and \
            i['UniqueID'] == '%s:SE:%s' % (i['Name'], i['Name'])]
        return result

    def areas(self, se):
        info = [i for i in self.sas.values() if i['SpaceType'] == 'Area' \
            and i['ParentID'] == se]
        self._log.info("Areas we have recorded for SE %s: %s" % (se,
            ', '.join([i['UniqueID'] for i in info])))
        return info

    def pools(self, se):
        info = [i for i in self.sas.values() if i['SpaceType'] == 'Pool' \
            and i['ParentID'] == se]
        self._log.info("Pools we have recorded for SE %s: %s" % (se,
            ', '.join([i['UniqueID'] for i in info])))
        return info

    def paths(self, se, area):
        results = []
        for entry in self.sas.values():
            if entry['SpaceType'] not in ['Directory', 'Quota']:
                continue
            parent = entry['ParentID']
            if parent != area:
                continue
            if self.sas.get(parent, {}).get('ParentID', None) == se:
                results.append(entry)
        return results

    def cmds(self, se):
        return self._custom.get(se, {})

class Report(object):

    def __init__(self, conn, startDate, logger, cp):
        self._conn = conn
        self._startDate = startDate
        self._logger = logger
        self._cp = cp
        try:
            self._se_name = cp.get("Gratia", "SiteName")
        except:
            self._se_name = socket.getfqdn()
        self._today = SEInfo(self._se_name, startDate, conn, logger)
        self._yesterday = SEInfo(self._se_name, startDate - \
            datetime.timedelta(1, 0), conn, logger)
        self._week = SEInfo(self._se_name, startDate - datetime.timedelta(7,
            0), conn, logger)
        ses = self._today.SEs()
        if len(ses) > 1:
            raise Exception("SE name %s matches multiple SEs." % self._se_name)
        elif len(ses) == 0:
            raise Exception("SE name %s matches no SE." % self._se_name)
        self._se = ses[0]

    def title(self):
        if 'UsedSpace' not in self._se or 'TotalSpace' not in self._se:
            perc = 'UNKNOWN'
        else:
            TotalSpace, UsedSpace = self._se['TotalSpace'], \
                self._se['UsedSpace']
            if TotalSpace == 0:
                perc = "UNKNOWN"
            else:
                perc = str(int(round(UsedSpace/float(TotalSpace)*100)))
        title = 'The %s Chronicle | %s %% | %s' % (self._se['Implementation'],
            perc, self._startDate.strftime('%Y-%m-%d'))
        return title

    subject = title

    def _getSeAttr(self, obj, attr):
         try:
             return obj.SEs()[0][attr]
         except:
             return 'UNKNOWN'

    def _getAllSe(self, attr):
         return [GB(self._getSeAttr(self._today, attr)), GB(self._getSeAttr( \
             self._yesterday, attr)), GB(self._getSeAttr(self._week, attr))]

    def generatePlain(self):
        text = '%s\n  %s\n%s\n\n' % ('='*60, self.title(), '='*60)
        text += '%s\n| Global Storage   |\n' % ('-'*20)
        
        table = make_table.Table(add_numbers=False)
        table.setHeaders(['', 'Today', 'Yesterday', 'One Week'])
        used = self._getAllSe('UsedSpace')
        total = self._getAllSe('TotalSpace')
        table.addRow(['Total Space (GB)'] + total)
        table.addRow(['Free Space (GB)'] + self._getAllSe('FreeSpace'))
        table.addRow(['Used Space (GB)'] + used)
        used_perc = ['UNKNOWN', 'UNKNOWN', 'UNKNOWN']
        for i in range(3):
            if used[i] != 'UNKNOWN' and total[i] != 'UNKNOWN' and total[i] > 0:
                used_perc[i] = '%i%%' % round(100*used[i]/float(total[i]))
        table.addRow(['Used Percentage'] + used_perc)
        text += table.plainText() + '\n'

        areas = self._today.areas(self._se['UniqueID'])
        area_dict = dict([(i['UniqueID'], i) for i in areas])
        area_keys = area_dict.keys()
        area_keys.sort()
        for key in area_keys:
            area = area_dict[key]
            dashes = '-' * (len(area['Name']) + 4)
            text += "%s\n| %s |\n" % (dashes, area['Name'])
            table = make_table.Table(add_numbers=False)

            # Determine if we have any quotas at all
            has_space_quota = False
            has_file_quota = False
            has_file_count = False
            for info in self._today.paths(self._se['UniqueID'],
                    area['UniqueID']):
                if info['FileCountLimit']:
                    has_file_quota = True
                if info['FreeSpace']:
                    has_space_quota = True
                if info['FileCount']:
                    has_file_count = True

            # Determine headers; removing those for quotas as necessary.
            headers = ['Path', 'Size(GB)', '1 Day Change', '7 Day Change']
            if has_space_quota:
                headers += ['Remaining']
            if has_file_count:
                headers += ['# Files', '1 Day Change', '7 Day Change']
                if has_file_quota:
                    headers += ['Remaining']
            table.setHeaders(headers)

            # Add a table row for each entry in the area.
            for info in self._today.paths(self._se['UniqueID'],
                    area['UniqueID']):
                path = info['UniqueID']
                yesterday_paths = self._yesterday.paths(self._se['UniqueID'],
                    area['UniqueID'])
                yesterday = None
                for entry in yesterday_paths:
                     if entry['UniqueID'] == path:
                         yesterday = entry
                         break
                week_paths = self._week.paths(self._se['UniqueID'],
                    area['UniqueID'])
                week = None
                for entry in week_paths:
                    if entry['UniqueID'] == path:
                        week = entry
                        break
                row_info = [info['Name'], GB(info['UsedSpace']), "UNKNOWN",
                    "UNKNOWN", "NO QUOTA", info['FileCount'], "UNKNOWN",
                    "UNKNOWN", "NO QUOTA"]
                if yesterday != None:
                    row_info[2] = GB(info['UsedSpace']-yesterday['UsedSpace'])
                    if info['FileCount'] != None and yesterday['FileCount'] \
                            != None:
                        row_info[6] = info['FileCount']-yesterday['FileCount']
                    else:
                        row_info[6] = 'UNKNOWN'
                if week != None:
                    row_info[3] = GB(info['UsedSpace']-week['UsedSpace'])
                    if info['FileCount'] != None and week['FileCount'] != None:
                        row_info[7] = info['FileCount']-week['FileCount']
                    else:
                        row_info[7] = 'UNKNOWN'
                if info['FileCountLimit'] != None and info['FileCountLimit'] \
                        > 0:
                    row_info[-1] = info['FileCountLimit'] - info['FileCount']
                if info['FreeSpace']:
                    row_info[4] = GB(info['FreeSpace'])
                #else:
                #    row_info[4] = GB(self._se['FreeSpace'])
                if not has_space_quota:
                    row_info.pop(4)
                if not has_file_quota:
                    row_info.pop(-1)
                if not has_file_count:
                    if has_space_quota:
                        row_info = row_info[:5]
                    else:
                        row_info = row_info[:4]
                table.addRow(row_info)
            text += table.plainText() + '\n'

        name = "Pool Information"
        dashes = '-' * (len(name) + 4)
        text += "%s\n| %s |\n" % (dashes, name)
        def make_pool_info(day):
            day_pools = day.pools(self._se['UniqueID'])
            day_pools = [i for i in day_pools if i['Status'] == 'Production']
            day_size = [i['UsedSpace']/float(i['TotalSpace']) for i in \
                day_pools if i['TotalSpace']]
            day_poolnames = sets.Set([i['Name'] for i in day_pools])
            day_avg = avg(day_size)
            day_stddev = stddev(day_size)
            return day_poolnames, day_avg, day_stddev
        today_poolnames, today_avg, today_stddev = make_pool_info(self._today)
        today_dead = self._today.pools(self._se['UniqueID'])
        today_dead = [i for i in today_dead if i['Status'] != 'Production']
        yest_poolnames, yest_avg, yest_stddev = make_pool_info(self._yesterday)
        yest_dead = self._yesterday.pools(self._se['UniqueID'])
        yest_dead = [i for i in yest_dead if i['Status'] != 'Production']
        week_poolnames, week_avg, week_stddev = make_pool_info(self._week)
        week_dead = self._week.pools(self._se['UniqueID'])
        week_dead = [i for i in week_dead if i['Status'] != 'Production']
        table = make_table.Table(add_numbers=False)
        table.setHeaders(['Statistic', 'Today', '1 Day Change', '7 Day Change'])
        table.addRow(['Online Pool Count', len(today_poolnames),
            len(today_poolnames) - len(yest_poolnames),
            len(today_poolnames)-len(week_poolnames)])
        table.addRow(['Offline Pool Count', len(today_dead),
            len(today_dead) - len(yest_dead),
            len(today_dead) - len(week_dead)])
        try:
            tmp1 = '%i%%' % round(100*today_avg)
        except:
            tmp1 = 'UNKNOWN'
        try:
            tmp2 = '%i%%' % round(100*(today_avg-yest_avg))
        except:
            tmp2 = 'UNKNOWN'
        try:
            tmp3 = '%i%%' % round(100*(today_avg-week_avg))
        except:
            tmp3 = 'UNKNOWN'
        table.addRow(['% Used Avg', tmp1, tmp2, tmp3])
        try:
            tmp1 = '%i%%' % round(100*today_stddev)
        except:
            tmp1 = 'UNKNOWN'
        try:
            tmp2 = '%i%%' % round(100*(today_stddev-yest_stddev))
        except:
            tmp2 = 'UNKNOWN'
        try:
            tmp3 = '%i%%' % round(100*(today_stddev-week_stddev))
        except:
            tmp3 = 'UNKNOWN'
        table.addRow(['% Used Std Dev', tmp1, tmp2, tmp3])
        text += table.plainText()
        new_pools_today = today_poolnames.difference(yest_poolnames)
        new_pools_week = today_poolnames.difference(week_poolnames)
        dead_pools_today = yest_poolnames.difference(today_poolnames)
        dead_pools_week = week_poolnames.difference(today_poolnames)
        if new_pools_today:
            text += "New pools today: %s\n" % ", ".join(new_pools_today)
        else:
            text += "No new pools today.\n"
        if new_pools_week:
            text += "New pools this week: %s\n" % ", ".join(new_pools_week)
        else:
            text += "No new pools this week.\n"
        if dead_pools_today:
            text += "New missing/dead pools today: %s\n" % ", ".join( \
                dead_pools_today)
        else:
            text += "No new dead pools today.\n"
        if dead_pools_week:
            text += "New missing/dead pools this week: %s\n" % ", ".join( \
                dead_pools_week)
        else:
            text += "No new dead pools this week.\n"
        text += "\n"

        for name, output in self._today.cmds(self._se['UniqueID']).items():
            dashes = '-' * (len(name) + 4)
            text += "%s\n| %s |\n%s\n" % (dashes, name, dashes)
            text += output + '\n'

        self._logger.info("\n" + text)
        return text

    def generateHtml(self):
        return '<pre>\n%s\n</pre>\n' % self.generatePlain()

    def name(self):
        return "site_storage_report"

