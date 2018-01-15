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

def extractOrganization(data):
  data = json.loads(data)
  organizations = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'entities':
        if data[key]['_type'] == 'Organization':
          organizations.append({'name': data[key]['name'].replace('\'', '\\\''), 'nationality': data[key]['nationality'], 'organizationtype': data[key]['organizationtype'], 'relevance': data[key]['relevance']})
    except:
      pass

  return organizations

def extractCompany(data):
  data = json.loads(data)
  companies = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'entities':
        if data[key]['_type'] == 'Company':
          companies.append({'name': data[key]['name'].replace('\'', '\\\''), 'nationality': data[key]['nationality'], 'relevance': data[key]['relevance'], 'confidencelevel': data[key]['confidencelevel']})
    except:
      pass
  
  return companies

def extractPerson(data):
  data = json.loads(data)
  persons = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'entities':
        if data[key]['_type'] == 'Person':
          persons.append({'name': data[key]['name'].replace('\'', '\\\''), 'persontype': data[key]['persontype'], 'nationality': data[key]['nationality'], 'commonname': data[key]['commonname'].replace('\'', '\\\''), 'relevance': data[key]['relevance'], 'confidencelevel': data[key]['confidencelevel']})
    except:
      pass

  return persons

def extractCountry(data):
  data = json.loads(data)
  countries = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'entities':
        if data[key]['_type'] == 'Country':
          countries.append({'name': data[key]['name'].replace('\'', '\\\''), 'relevance': data[key]['relevance']})
    except:
      pass

  return countries

def extractCity(data):
  data = json.loads(data)
  cities = []
  for key in data:
    try:
      typeGroup = data[key]['_typeGroup']
      if typeGroup == 'entities':
        if data[key]['_type'] == 'City':
          cities.append({'name': data[key]['name'].replace('\'', '\\\''), 'relevance': data[key]['relevance']})
    except:
      pass
  
  return cities

tableName = "dump"
schemaCategory = "CREATE TABLE IF NOT EXISTS calais_category ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, score FLOAT )"
insertCategoryQuery = Template("INSERT INTO calais_category (news_id, name, score) VALUES ('$news_id', '$name', $score)")

schemaEntity = "CREATE TABLE IF NOT EXISTS calais_entity ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, type TEXT, relevance FLOAT )"
insertEntityQuery = Template("INSERT INTO calais_entity (news_id, name, type, relevance) VALUES ('$news_id', '$name', '$type', $relevance)")

schemaOrganization = "CREATE TABLE IF NOT EXISTS calais_organization ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, nationality TEXT, organizationtype TEXT, relevance FLOAT )"
insertOrganizationQuery = Template("INSERT INTO calais_organization (news_id, name, nationality, organizationtype, relevance) VALUES ('$news_id', '$name', '$nationality', '$organizationtype', $relevance)")

schemaCompany = "CREATE TABLE IF NOT EXISTS calais_company ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, nationality TEXT, relevance FLOAT, confidencelevel FLOAT )"
insertCompanyQuery = Template("INSERT INTO calais_company (news_id, name, nationality, relevance, confidencelevel) VALUES ('$news_id', '$name', '$nationality', $relevance, $confidencelevel)")

schemaPerson = "CREATE TABLE IF NOT EXISTS calais_person ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, persontype TEXT, nationality TEXT, commonname TEXT CHARACTER SET utf8mb4, relevance FLOAT, confidencelevel FLOAT )"
insertPersonQuery = Template("INSERT INTO calais_person (news_id, name, persontype, nationality, commonname, relevance, confidencelevel) VALUES ('$news_id', '$name', '$persontype', '$nationality', '$commonname', $relevance, $confidencelevel)")

schemaCountry = "CREATE TABLE IF NOT EXISTS calais_country ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, relevance FLOAT )"
insertCountryQuery = Template("INSERT INTO calais_country (news_id, name, relevance) VALUES ('$news_id', '$name', $relevance)")

schemaCity = "CREATE TABLE IF NOT EXISTS calais_city ( news_id TEXT, name TEXT CHARACTER SET utf8mb4, relevance FLOAT )"
insertCityQuery = Template("INSERT INTO calais_city (news_id, name, relevance) VALUES ('$news_id', '$name', $relevance)")

def run(conn) :
  cursor = conn.cursor()
  cursor.execute(schemaCategory)
  cursor.execute(schemaEntity)
  cursor.execute(schemaOrganization)
  cursor.execute(schemaCompany)
  cursor.execute(schemaPerson)
  cursor.execute(schemaCountry)
  cursor.execute(schemaCity)

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
    insertCursor = conn.cursor()
    content = title + body
    data = analyze(content.encode('utf-8')).text

    print("Category:")
    categories = extractCategory(data)
    for category in categories:
      print("\t", category["name"], category["score"])
      query = insertCategoryQuery.substitute(news_id=news_id, name=category["name"], score=category["score"])
      insertCursor.execute(query)
    print('------------------------------------------')

    print("Entity:")
    entities = extractEntity(data)
    for entity in entities:
      print("\t", entity["name"], entity["type"], entity["relevance"])
      query = insertEntityQuery.substitute(news_id=news_id, name=entity["name"], type=entity["type"], relevance=entity["relevance"])
      insertCursor.execute(query)
    print()

    print("Organization:")
    organizations = extractOrganization(data)
    for org in organizations:
      print('\t', org['name'], org['nationality'], org['organizationtype'], org['relevance'])
      query = insertOrganizationQuery.substitute(news_id=news_id, name=org["name"], nationality=org["nationality"], organizationtype=org["organizationtype"], relevance=org["relevance"])
      insertCursor.execute(query)
    print()

    print("Company:")
    companies = extractCompany(data)
    for cpn in companies:
      print('\t', cpn['name'], cpn['nationality'], cpn['relevance'], cpn['confidencelevel'])
      query = insertCompanyQuery.substitute(news_id=news_id, name=cpn["name"], nationality=cpn["nationality"], relevance=cpn["relevance"], confidencelevel=cpn["confidencelevel"])
      insertCursor.execute(query)
    print()

    print("Person:")
    persons = extractPerson(data)
    for per in persons:
      print('\t', per['name'], per['persontype'], per['nationality'], per['commonname'], per['relevance'], per['confidencelevel'])
      query = insertPersonQuery.substitute(news_id=news_id, name=per["name"], persontype=per["persontype"], nationality=per["nationality"], commonname=per["commonname"], relevance=per["relevance"], confidencelevel=per["confidencelevel"])
      insertCursor.execute(query)
    print()

    print("Countries:")
    countries = extractCountry(data)
    for cou in countries:
      print('\t', cou['name'], cou['relevance'])
      query = insertCountryQuery.substitute(news_id=news_id, name=cou["name"], relevance=cou["relevance"])
      insertCursor.execute(query)
    print()

    print("Cities:")
    cities = extractCity(data)
    for cty in cities:
      print('\t', cty['name'], cty['relevance'])
      query = insertCityQuery.substitute(news_id=news_id, name=cty["name"], relevance=cty["relevance"])
      insertCursor.execute(query)
    print()

    print("")
    conn.commit()
    time.sleep(0.5)
