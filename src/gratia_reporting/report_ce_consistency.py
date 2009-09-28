
import time
import urllib2
import datetime
from xml.dom.minidom import parse

import gratia_reporting.make_table as make_table

CE_query = """
SELECT
  HostName, SiteName
FROM ComputeElement FORCE INDEX(Timestamp)
WHERE
  ProbeName like 'gip%%' AND
  Timestamp <= %s
GROUP BY HostName
HAVING MAX(Timestamp)
"""

recent_CE_query = """
SELECT
  CE.HostName, CE.SiteName
FROM ComputeElementRecord CER FORCE INDEX(Timestamp)
JOIN (
  SELECT
    UniqueID, HostName, SiteName
  FROM ComputeElement FORCE INDEX(Timestamp)
  WHERE
    ProbeName like 'gip%%' AND
    Timestamp <= %s
  GROUP BY HostName
  HAVING MAX(Timestamp)
) as CE on CE.UniqueID=CER.UniqueID
WHERE
  Timestamp > %s AND
  Timestamp <= %s
GROUP BY CER.UniqueID
"""

oim_url = "http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&" \
    "summary_attrs_showservice=on&summary_attrs_showrsvstatus=on&" \
    "summary_attrs_showfqdn=on&all_resources=on&gridtype=on&gridtype_1=on&" \
    "service=on&service_1=on&active_value=1&disable_value=1&" \
    "active=on&active_value=1&disable=on&disable_value=0"

class Report(object):

    def __init__(self, conn, startDate, logger, cp):
        self._db = conn
        self._startDate = startDate
        self._log = logger
        self._cp = cp
        self.parseOIM()
        self.parseGratia()
        self.parseRecentGratia()

    def _execute(self, stmt, *args):
        self._log.info(stmt % args)
        timer = -time.time()
        curs = self._db.cursor()
        curs.execute(stmt, args)
        timer += time.time()
        self._log.info("Query took %.2f seconds." % timer)
        return curs

    def name(self):
        return "ce_consistency"

    def parseGratia(self):
        self._gratiaCE = {}
        for host, site in self._execute(CE_query,
                self._startDate.strftime('%Y-%m-%d %H:%M:%S')):
            self._gratiaCE[host] = site

    def parseRecentGratia(self):
        self._recentGratiaCE = {}
        end = self._startDate.strftime('%Y-%m-%d %H:%M:%S')
        start = (self._startDate-datetime.timedelta(7, 0)).strftime('%Y-%m-%d' \
                ' %H:%M:%S')
        for host, site in self._execute(recent_CE_query, end, start,
                end):
            self._recentGratiaCE[host] = site

    def parseOIM(self):
        fp = urllib2.urlopen(oim_url)
        dom = parse(fp)
        self._oimCE = {}
        for resource_group_dom in dom.getElementsByTagName('ResourceGroup'):
            try:
                name = str(resource_group_dom.getElementsByTagName(\
                    'GroupName')[0].firstChild.data)
            except:
                continue
            for resource_dom in resource_group_dom.getElementsByTagName(\
                    'Resource'):
                try:
                    fqdn = str(resource_dom.getElementsByTagName('FQDN')[0].\
                        firstChild.data)
                except:
                    continue
                try:
                    hidden= str(resource_dom.getElementsByTagName(\
                        'HiddenService')[0].firstChild.data)
                except:
                    continue
                if hidden == 'True':
                    continue
                self._oimCE[fqdn] = name

    def inconsistent(self):
        entries = []
        for resource, oim_name in self._oimCE.items():
            if resource not in self._gratiaCE:
                continue
            gratia_name = self._gratiaCE[resource]
            if oim_name != gratia_name:
                entries.append([resource, oim_name, gratia_name])
        return entries

    def onlyGIP(self):
        entries = []
        for resource, gip_name in self._recentGratiaCE.items():
            if resource not in self._oimCE:
                entries.append([resource, gip_name])
        return entries

    def onlyOIM(self):
        entries = []
        for resource, oim_name in self._oimCE.items():
            if resource not in self._gratiaCE:
                entries.append([resource, oim_name])
        return entries

    def issueCount(self):
        return len(self.inconsistent()) + len(self.onlyGIP())

    def subject(self):
        return "CE Consistency Report for %s (%i Issues)" % (self._startDate.\
            strftime('%Y-%m-%d'), self.issueCount())

    def generatePlain(self):
        text = "%s\n" % self.subject()
        text += "\nThe following CE endpoints have inconsistent site names "\
            "between GIP and OIM:\n"
        table = make_table.Table()
        table.setHeaders(['CE', 'OIM Resource Group Name', 'GIP Site Name'])
        for entry in self.inconsistent():
            table.addRow(entry)
        text += table.plainText()
        if len(self.onlyGIP()) == 0:
            text += "\nThere are no CE endpoints in GIP but not OIM.\n"
        else:
            text += "\nThe following CE endpoints are in GIP but not OIM:\n"
            table2 = make_table.Table()
            table2.rowCtr = table.rowCtr
            table2.setHeaders(['Endpoint', 'GIP Site Name'])
            for entry in self.onlyGIP():
                table2.addRow(entry)
            text += table2.plainText()
        text += "\nThe following CE endpoints are in OIM but not GIP:\n"
        table3 = make_table.Table()
        if len(self.onlyGIP()) == 0:
            table3.rowCtr = table.rowCtr
        else:
            table3.rowCtr = table2.rowCtr
        table3.setHeaders(['Endpoint', 'OIM Resource Group Name'])
        for entry in self.onlyOIM():
            table3.addRow(entry)
        text += table3.plainText()
        print text
        return text

    def generateHtml(self):
        return '<pre>\n%s\n</pre>\n' % self.generatePlain()

