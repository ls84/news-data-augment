import requests
from string import Template
import random
import json
import time

def analyze(data):
  calais_url = 'https://api.thomsonreuters.com/permid/calais'
  token = 'eBMekewSABJPMkZD28nDM5IEdlwnVVks'
  headers = {'x-ag-access-token': token, 'Content-Type': 'text/raw', 'omitOutputtingOriginalText': 'true', 'outputFormat': 'application/json'}

  response = requests.post(calais_url, data=data, headers=headers, timeout=80)
  return response

def extractCategory(data):
  data = json.loads(data)
  categories = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'topics':
        categories.append({'name': data[key]['name'].replace('\'', '\\\''), 'score': data[key]['score']})
    except:
      pass

  if len(categories) == 0:
    categories.append({'name':'undefined', 'score': 0})

  return categories

def extractEntity(data):
  data = json.loads(data)
  entities = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'entities':
        entities.append({'name': data[key]['name'].replace('\'', '\\\''), "type": data[key]['_type'], 'relevance': data[key]['relevance']})
    except:
      pass

  return entities

tableName = "dump"
schemaCategory = "CREATE TABLE IF NOT EXISTS calais_category ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, score FLOAT )"
insertCategoryQuery = Template("INSERT INTO calais_category (news_id, name, score) VALUES ('$news_id', '$name', $score)")

schemaEntity = "CREATE TABLE IF NOT EXISTS calais_entity ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, type TEXT, relevance FLOAT )"
insertEntityQuery = Template("INSERT INTO calais_entity (news_id, name, type, relevance) VALUES ('$news_id', '$name', '$type', $relevance)")

def run(conn) :
  cursor = conn.cursor()
  cursor.execute(schemaCategory)
  cursor.execute(schemaEntity)

  SelectQuery = Template("SELECT R.news_id, L.title, R.body FROM $tableName AS L RIGHT JOIN body AS R ON L.id=R.news_id LEFT JOIN calais_category ON L.id=calais_category.news_id WHERE calais_category.news_id IS NULL").substitute(tableName=tableName)
  cursor.execute(SelectQuery)
  # NOTE: for debug
  # result = list(cursor.fetchall())
  # random.shuffle(result)
  result = cursor.fetchall()
  total = len(result)
  counter = 0
  for (news_id, title, body) in result:
    counter += 1
    print(Template(("$counter / $total -> ")).substitute(total=total, counter=counter), news_id)
    print('##########################################')
    content = title + body
    data = analyze(content.encode('utf-8')).text
    categories = extractCategory(data)
    entities = extractEntity(data)
    insertCursor = conn.cursor()
    for category in categories:
      query = insertCategoryQuery.substitute(news_id=news_id, name=category["name"], score=category["score"])
      print(query)
      insertCursor.execute(query)
    print('------------------------------------------')
    for entity in entities:
      query = insertEntityQuery.substitute(news_id=news_id, name=entity["name"], type=entity["type"], relevance=entity["relevance"])
      print(query)
      insertCursor.execute(query)

    print("")
    conn.commit()
    time.sleep(0.5)
