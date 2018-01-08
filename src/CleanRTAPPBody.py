from bs4 import BeautifulSoup
from string import Template

schema = "CREATE TABLE IF NOT EXISTS body (news_id TEXT, body TEXT CHARACTER SET utf8mb4)"
insertQuery = Template("INSERT INTO body (news_id, body) VALUES ('$news_id', '$body')") 

def remove_tags(text):
  soup = BeautifulSoup(text, 'html.parser')
  return soup.get_text()

# TODO: dump is too generic
tableName = "dump"

def fix(conn):
  cursor = conn.cursor()
  cursor.execute(schema)
  conn.commit()

  selectQuery = Template("SELECT L.id, L.body FROM $tableName AS L LEFT JOIN body AS R ON L.id=R.news_id WHERE L.source_name='RT' AND L.actual_source_name='APP' AND R.news_id IS NULL"). substitute(tableName=tableName)
  print(selectQuery)
  cursor.execute(selectQuery)
  result = cursor.fetchall()
  for (id, body) in result:
    print("")
    print(id)
    print("------------------------------------------")
    print(body)
    insert = conn.cursor()
    cleanedBody = remove_tags(body).replace('\'', '\\\'').replace('\n', '')
    insert.execute(insertQuery.substitute(news_id=id, body=cleanedBody))
    conn.commit()
    print('vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv')
    print(cleanedBody)
    print("##########################################")
