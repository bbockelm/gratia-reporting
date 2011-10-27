
from distutils.core import setup

setup(name="GratiaReporting",
  version="0.3.8",
  author="Brian Bockelman",
  author_email="bbockelm@cse.unl.edu",
  description="Gratia reporting package.",

  package_dir={"": "src"},
  packages=["gratia_reporting"],

  data_files = [ \
    ("/etc/gratia_reporting/", ["conf/logging.cfg",
      "conf/reporting.cfg.template", "conf/gratia_reporting.cron"]),
  ],

  scripts = ['src/scripts/gratia_report']

)
