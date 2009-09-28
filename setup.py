
import distutils

setup(name="GratiaReporting",
  version="0.1",
  author="Brian Bockelman",
  author_email="bbockelm@cse.unl.edu",
  description="Gratia reporting package.",

  package_dir={"", "src"},
  packages=["gratia_reporting"],

  data_files = [ \
    ("/etc/gratia_reporting/", ["conf/logging.cfg", "conf/reporting.cfg",
      "conf/gratia_reporting.cron"]),
  ],

)
