import MySQLdb
from string import Template
tableName = 'dump'

# NOTE: depends on body table is already existed
InsertCNN = Template("INSERT INTO body (news_id, body) SELECT L.id, L.body FROM $tableName AS L LEFT JOIN body AS R ON L.id=R.news_id WHERE L.source_name='CNN' AND actual_source_name='APP' AND R.news_id IS NULL").substitute(tableName=tableName)

InsertAJ = Template("INSERT INTO body (news_id, body) SELECT L.id, L.body FROM $tableName AS L LEFT JOIN body AS R ON L.id=R.news_id WHERE L.source_name='AJ' AND actual_source_name='APP' AND R.news_id IS NULL").substitute(tableName=tableName)

Alter = "ALTER TABLE body ADD word_count INT UNSIGNED"
Select = "Select news_id, body FROM body WHERE word_count IS NULL"
Update = Template("UPDATE body SET word_count = $word_count WHERE news_id = '$news_id'")
def fix(conn):
  InsertCursor = conn.cursor()
  InsertCursor.execute(InsertCNN)
  InsertCursor.execute(InsertAJ)

  try:
    cursor = conn.cursor()
    cursor.execute(Alter)
  except MySQLdb.Error as e:
    if e.args[0] != 1060:
      raise Exception(e)
    pass

  cursor.execute(Select)
  result = cursor.fetchall()
  for (news_id, body) in result:
    word_count = len(body.split())
    updateCursor = conn.cursor()
    updateStatement = Update.substitute(word_count=word_count, news_id=news_id)
    print(updateStatement)
    updateCursor.execute(updateStatement)
    conn.commit()
