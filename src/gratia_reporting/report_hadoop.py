#!/bin/env python
 
import sys
import time
import types
import socket
import datetime
import xml.dom.minidom

import gratia_reporting.make_table as make_table

SER_query = """
SELECT
  SE.UniqueID, SE.ParentID, SE.Name, SE.SpaceType, SE.Implementation, SE.Version,
  SER.MeasurementType, SER.TotalSpace, SER.FreeSpace, SER.UsedSpace, SER.FileCount, SER.FileCountLimit
FROM
  (SELECT * from (SELECT
     Timestamp, UniqueID, MeasurementType, TotalSpace, FreeSpace, UsedSpace, FileCount, FileCountLimit
   FROM StorageElementRecord FORCE INDEX(Timestamp)
   WHERE
     DATE(Timestamp) = %s
   GROUP BY UniqueID, MeasurementType
  ) as SER2 GROUP BY UniqueID, MeasurementType) as SER
JOIN (SELECT * from StorageElement WHERE DATE(Timestamp) <= %s AND SE=%s GROUP BY UniqueID HAVING MAX(Timestamp)) as SE on SE.UniqueID=SER.UniqueID
"""

parents_query = """
SELECT
  SE.UniqueID,
  SE.ParentID,
  SE.Name,
  SE.SpaceType,
  SE.Version,
  SE.SiteName,
  SE.Implementation
FROM 
  (SELECT
     dbid,
     UniqueID,
     COALESCE(Timestamp, '1970-01-01 00:00:00') as time
   FROM StorageElement force index (Timestamp)
   WHERE
     (DATE(Timestamp) <= %%s OR Timestamp IS NULL)
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
  DATE(EndTime) = %s
GROUP BY Name, SEUniqueID
HAVING MAX(EndTime)
"""

def GB(bytes):
    try:
        return int(round(float(bytes)/1000.**3))
    except:
        return 'UNKNOWN'

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
        self.results = self._execute(SER_query, date, date,
            self._se_name).fetchall()
        for result in self.results:
            uniqId, parentId, name, spaceType, implementation, version, \
                measurementType, totalSpace, freeSpace, usedSpace, fileCount, \
                fileCountLimit = result
            info = {'UniqueID': uniqId, 'ParentID': parentId, 'Name': name,
                'SpaceType': spaceType, 'Implementation': implementation,
                'Version': version, 'MeasurementType': measurementType,
                'TotalSpace': totalSpace, 'FreeSpace': freeSpace, 'FileCount':\
                fileCount, 'FileCountLimit': fileCountLimit, 'UsedSpace': \
                usedSpace}
            if info['MeasurementType'] == 'logical' and info['SpaceType'] == 'SE':
                #print info
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
            '%Y-%m-%d'), *needed_parents).fetchall()
        for result in self.results:
            uniqID, parentID, name, spaceType, version, siteName, impl \
                = result
            info = {'UniqueID': uniqID, 'ParentID': parentID, 'Name': name,
                'SpaceType': spaceType, 'Version': version}
            self.sas[info['UniqueID']] = info

    def query_cmds(self):
        date = self._date.strftime("%Y-%m-%d")
        results = self._execute(cmds_query, date)
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
            if used[i] != 'UNKNOWN' and total[i] != 'UNKNOWN':
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
            table.setHeaders(['Path', 'Size(GB)', '1 Day Change',
                '7 Day Change', 'Remaining', '# Files', '1 Day Change',
                '7 Day Change', 'Remaining'])
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
                week_paths = self._week.paths(self._se['UniqueID'], area['UniqueID'])
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
                    row_info[6] = info['FileCount']-yesterday['FileCount']
                if week != None:
                    row_info[3] = GB(info['UsedSpace']-week['UsedSpace'])
                    row_info[7] = info['FileCount']-week['FileCount']
                if info['FileCountLimit'] != None and info['FileCountLimit'] \
                        > 0:
                    row_info[-1] = info['FileCountLimit'] - info['FileCount']
                if info['FreeSpace']:
                    row_info[4] = GB(info['FreeSpace'])
                #else:
                #    row_info[4] = GB(self._se['FreeSpace'])
                table.addRow(row_info)
            text += table.plainText() + '\n'

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

