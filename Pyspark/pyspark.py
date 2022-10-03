from queue import Empty
import findspark 
import mysql.connector
import os
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
      
db = mysql.connector.MySQLConnection(
    host="localhost",
    user="root",
    password="Setiawan112!",
    database="proj1"
)
  
#the tables
df1 = spark.read.json(path = "C:\\Users\\Fenix Xia\\Desktop\\Pyspark\\response.json")
df1.createOrReplaceTempView("Movies")
df2 = spark.read.json(path = "C:\\Users\\Fenix Xia\\Desktop\\Pyspark\\genres.json")
df2.createOrReplaceTempView("Genres")
#start of query methods
def getTables():
  df1.show()
  df2.show()

def querySpecific(name):
  spark.sql("Select * from Movies where title = '{}'".format(name)).show()

def queryPopular():
  spark.sql("Select * from Movies WHERE popularity = (SELECT MAX(popularity) FROM Movies)").show()
  
def queryGenre(name):
  from pyspark.sql.functions import explode
  df3 = df1.select(df1.title, explode(df1.genre_ids).alias("id"))
  df3.createOrReplaceTempView("explode")
  spark.sql("Select * from explode join Genres where explode.id = Genres.id").createOrReplaceTempView("combined")
  spark.sql("Select title, name from combined where name = '{}'".format(name)).show(1000)

def queryDate(year1, year2):
  from pyspark.sql.functions import to_date, col
  df1.select("title", to_date(col("release_date"), "yyyy-MM-dd").alias("date")).createOrReplaceTempView("calendar")
  spark.sql("Select * from calendar WHERE date >= '{}-01-01' AND date < '{}-12-31' order by date desc".format(year1, year2)).show(1000)

def queryCount():
  from pyspark.sql.functions import explode
  df3 = df1.select(df1.title, explode(df1.genre_ids).alias("id"))
  df3.createOrReplaceTempView("explode")
  spark.sql("Select * from explode join Genres where explode.id = Genres.id").createOrReplaceTempView("combined")
  spark.sql("Select name, count(name) as count from combined group by name order by count desc").show()
def queryWeighted():
  spark.sql("SELECT title, cast(sum(vote_average * vote_count) / sum(vote_count) as decimal(8,2)) as weighted_average FROM Movies group by title order by weighted_average desc").show()
#end of query methods
#start of mysql methods
def getall():
  mycursor = db.cursor()
  mycursor.execute("Select * from users")
  result = mycursor.fetchall()
  for i in result:
    print(i)
  
def insertuser(name, username, password):
  mycursor = db.cursor()
  sql = "INSERT INTO USERS (name, username, password, role) VALUES ('{}','{}','{}','user')".format(name, username, password)
  mycursor.execute(sql)
  db.commit()
  
def checkuser(username, password):
  mycursor = db.cursor()
  mycursor.execute("select * from users where username ='{}' And password ='{}'".format(username, password))
  userline = mycursor.fetchall()
  print(userline)
  if len(userline) == 0:
    return False
  else:
    return True

def getrole(username, password):
  mycursor = db.cursor()
  mycursor.execute("select role from users where username ='{}' And password ='{}'".format(username, password))
  userline = mycursor.fetchone()
  if userline == None:
    return False
  for i in userline:
    if i == "user":
      return "user"
    elif i == "admin":
      return "admin"
  
print(getrole("Kev", "12"))

if __name__ == "__main__":
  while True:
    print("welcome to moviefinder (PY VERSION)")
    print("1 to make account")
    print("2 to login")
    print("3 to exit")
    choice = input("Input here: ")
    
    if(choice == "3"):
      print("exiting....")
      exit()
    elif(choice == "2"):
      username= input("username: ")
      password = input("Password: ")
      if(checkuser(username, password) == True and getrole(username, password) == "user"):
        print("welcome "+username)
        exit()
      elif(checkuser(username, password) == True and getrole(username, password) == "admin"):
        print("welcome admin"+username)
        exit()
      else:
        print("You can try again...")
    elif(choice == "1"):
      name = input("What is your name: ")
      username= input("what would u like ur username to be: ")
      password = input("Password?: ")
      insertuser(name, username, password)
      exit()
    else:
      print("----------------")
      print("invalid choice")
      print("----------------")








