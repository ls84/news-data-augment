#! /usr/local/bin/python

import MySQLdb
import CompleteCGTNBody
import CleanBBCAPPBody
import CleanRTAPPBody
import CalculateAPPBodyWordCount
import TagAPPContentUsingCalais

# TODO:
# SeparateCGTNAPPSources


config = {
  "host": "mysql-server",
  "user": "root",
  "passwd": "123456",
  "db": "news",
  "charset": "utf8"
}
conn = MySQLdb.connect(**config)

CompleteCGTNBody.fix(conn)
CleanBBCAPPBody.fix(conn)
CleanRTAPPBody.fix(conn)
CalculateAPPBodyWordCount.fix(conn)
TagAPPContentUsingCalais.run(conn)
