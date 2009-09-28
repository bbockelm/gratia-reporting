
import time
import urllib2
import datetime
from xml.dom.minidom import parse

import gratia_reporting.make_table as make_table

SE_query = """
SELECT
  UniqueID, Name, Implementation
FROM StorageElement FORCE INDEX(Timestamp)
WHERE
  SpaceType='SE' AND
  ProbeName like 'gip%%' AND
  Timestamp <= %s
GROUP BY UniqueID
HAVING MAX(Timestamp)
"""

recent_SE_query = """
SELECT
  SER.UniqueID, SE.Name, SE.Implementation
FROM StorageElementRecord SER FORCE INDEX(Timestamp)
JOIN (
  SELECT
    UniqueID, Name, Implementation
  FROM StorageElement FORCE INDEX(Timestamp)
  WHERE
    SpaceType='SE' AND
    ProbeName like 'gip%%' AND
    Timestamp <= %s
  GROUP BY UniqueID
  HAVING MAX(Timestamp)
) as SE on SE.UniqueID=SER.UniqueID
WHERE
  Timestamp > %s AND
  Timestamp <= %s
GROUP BY UniqueID
"""

oim_url = "http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&" \
    "summary_attrs_showservice=on&summary_attrs_showrsvstatus=on&" \
    "summary_attrs_showfqdn=on&all_resources=on&gridtype=on&gridtype_1=on&" \
    "service=on&service_4=on&service_2=on&service_3=on&active_value=1&" \
    "disable_value=1"

class Report(object):

    def __init__(self, conn, startDate, logger, cp):
        self._db = conn
        self._startDate = startDate
        self._log = logger
        self._cp = cp
        self.parseGratia()
        self.parseRecentGratia()
        self.parseOIM()

    def _execute(self, stmt, *args):
        self._log.info(stmt % args)
        timer = -time.time()
        curs = self._db.cursor()
        curs.execute(stmt, args)
        timer += time.time()
        self._log.info("Query took %.2f seconds." % timer)
        return curs

    def name(self):
        return "se_consistency"

    def parseGratia(self):
        self._gratiaSE = {}
        for uniqId, name, impl in self._execute(SE_query,
                self._startDate.strftime('%Y-%m-%d %H:%M:%S')):
            impl = impl.lower()
            if impl.find('classic') >= 0 or impl.find('disk') >= 0 or impl.find('un') >= 0:
                continue
            endpoint = uniqId.split(':')[0]
            name = name.split(':')[0]
            self._gratiaSE[endpoint] = name

    def parseRecentGratia(self):
        self._recentGratiaSE = {}
        end = self._startDate.strftime('%Y-%m-%d %H:%M:%S')
        start = (self._startDate-datetime.timedelta(7, 0)).strftime('%Y-%m-%d' \
                ' %H:%M:%S')
        for uniqId, name, impl in self._execute(recent_SE_query, end, start,
                end):
            impl = impl.lower()
            if impl.find('classic') >= 0 or impl.find('disk') >= 0 or impl.find('un') >= 0:
                continue
            endpoint = uniqId.split(':')[0]
            name = name.split(':')[0]
            self._recentGratiaSE[endpoint] = name

    def parseOIM(self):
        fp = urllib2.urlopen(oim_url)
        dom = parse(fp)
        self._oimSE = {}
        for resource_dom in dom.getElementsByTagName('Resource'):
            try:
                fqdn = str(resource_dom.getElementsByTagName('FQDN')[0].\
                    firstChild.data)
                name = str(resource_dom.getElementsByTagName('Name')[0].\
                    firstChild.data)
            except:
                continue
            self._oimSE[fqdn] = name

    def inconsistent(self):
        entries = []
        for resource, oim_name in self._oimSE.items():
            if resource not in self._gratiaSE:
                continue
            gratia_name = self._gratiaSE[resource]
            if oim_name != gratia_name:
                entries.append([resource, oim_name, gratia_name])
        return entries

    def onlyGIP(self):
        entries = []
        for resource, gip_name in self._recentGratiaSE.items():
            if resource not in self._oimSE:
                entries.append([resource, gip_name])
        return entries

    def onlyOIM(self):
        entries = []
        for resource, oim_name in self._oimSE.items():
            if resource not in self._gratiaSE:
                entries.append([resource, oim_name])
        return entries

    def issueCount(self):
        return len(self.inconsistent()) + len(self.onlyGIP())

    def subject(self):
        return "SE Consistency Report for %s (%i Issues)" % (self._startDate.\
            strftime('%Y-%m-%d'), self.issueCount())

    def generatePlain(self):
        text = "%s\n" % self.subject()
        text += "\nThe following SRM endpoints have inconsistent names between"\
            " GIP and OIM:\n"
        table = make_table.Table()
        table.setHeaders(['Endpoint', 'OIM Resource Name', 'GIP SE Name'])
        for entry in self.inconsistent():
            table.addRow(entry)
        text += table.plainText()
        text += "\nThe following SRM endpoints are in GIP but not OIM:\n"
        table2 = make_table.Table()
        table2.rowCtr = table.rowCtr
        table2.setHeaders(['Endpoint', 'GIP SE Name'])
        for entry in self.onlyGIP():
            table2.addRow(entry)
        text += table2.plainText()
        text += "\nThe following SRM endpoints are in OIM but not GIP:\n"
        table3 = make_table.Table()
        table3.rowCtr = table2.rowCtr
        table3.setHeaders(['Endpoint', 'OIM Resource Name'])
        for entry in self.onlyOIM():
            table3.addRow(entry)
        text += table3.plainText()
        print text
        return text

    def generateHtml(self):
        return '<pre>\n%s\n</pre>\n' % self.generatePlain()

