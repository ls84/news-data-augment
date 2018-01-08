import urllib.request
from bs4 import BeautifulSoup
from string import Template
import time
import random
import json

schema = "CREATE TABLE IF NOT EXISTS body ( news_id TEXT, body TEXT CHARACTER SET utf8mb4 )"
insertQuery = Template("INSERT INTO body (news_id, body) VALUES ('$news_id', '$body')")

def remove_tags(text):
  soup = BeautifulSoup(text, 'html.parser')
  return soup.get_text()


# TODO: dump is too generic
tableName = "dump"
def fix(conn):
  cursor = conn.cursor()
  cursor.execute(schema)

  cursor.execute(Template("SELECT L.id, L.src_link FROM $tableName AS L LEFT JOIN body AS R ON L.id=R.news_id WHERE L.source_name='CGTN' AND actual_source_name='APP' AND R.news_id IS NULL").substitute(tableName=tableName))
  # NOTE: for debugging
  # result = list(cursor.fetchall())
  # random.shuffle(result)

  result = cursor.fetchall()
  total = len(result)
  counter = 0
  for (id, src_link) in result:
    counter += 1
    print(Template(("$counter / $total -> ")).substitute(total=total, counter=counter), id, src_link)
    print('##########################################')
    sauce = urllib.request.urlopen(src_link).read()
    soup = BeautifulSoup(sauce, 'html.parser')
    content = soup.find("div", class_="content")
    try:
      bodyText = ''
      jsonString = content["data-json"]
      for i in json.loads(jsonString):
        try:
          # NOTE: sometimes content is a dict for interactive stories
          if type(i["content"]) is str:
            bodyText += i["content"].strip()
        except KeyError:
          pass
      body = remove_tags(bodyText).replace('\'', '\\\'')
      print(body)
      insert = conn.cursor()
      insert.execute(insertQuery.substitute(news_id=id, body=body))
      conn.commit()
    except KeyError:
      insert = conn.cursor()
      insert.execute(insertQuery.substitute(news_id=id, body=""))
      conn.commit()
      pass
    print('')
    time.sleep(0.5)
