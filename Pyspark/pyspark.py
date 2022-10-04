import findspark 
import mysql.connector
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

def deleteuser(username, password):
  mycursor = db.cursor()
  sql = "DELETE FROM USERS WHERE username = '{}' and password = '{}'".format(username, password)
  mycursor.execute(sql)
  db.commit()

def deleteuseradmin(username):
  mycursor = db.cursor()
  sql = "DELETE FROM USERS WHERE username = '{}'".format(username)
  mycursor.execute(sql)
  db.commit()
  
def checkuser(username, password):
  mycursor = db.cursor()
  mycursor.execute("select * from users where username ='{}' And password ='{}'".format(username, password))
  userline = mycursor.fetchall()
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
        while True:
          print("welcome user "+username)
          print("what would you like to do")
          print("1 to exit")
          print("2 to look at tables used")
          print("3 find a specific movie")
          print("4 find most populat movie")
          print("5 find a movies based on genre")
          print("6 find movies between years")
          print("7 find movies based on genre count")
          print("8 delete account")
          choice = int(input("Input here: "))
          if choice == 1:
            quit()
          elif choice == 2:
            getTables()
          elif choice == 3:
            name = input("Name of the movie: ")
            querySpecific(name)
          elif choice == 4:
            queryPopular()
          elif choice == 5:
            name = input("Genre to lookup?: ")
            queryGenre(name)
          elif choice == 6:
            year1 = input("early year?: ")
            year2 = input("later year?: ")
            queryDate(year1, year2)
          elif choice == 7:
            queryCount()
          elif choice == 8:
            print("user deleted")
            deleteuser(username, password)
            quit()
            
      elif(checkuser(username, password) == True and getrole(username, password) == "admin"):
        while True:
          print("welcome admin "+username)
          print("1 delete a user based on username ")
          print("2 logout")
          choice = int(input("input here: "))
          if choice == 1:
            name = input("what is the username of the account to be deleted: ")
            deleteuseradmin(name)
          elif choice == 2:
            quit()
          
      else:
        print("You can try again...")
    elif(choice == "1"):
      name = input("What is your name: ")
      username= input("what would u like ur username to be: ")
      password = input("Password?: ")
      insertuser(name, username, password)
      print("user created")
    else:
      print("----------------")
      print("invalid choice")
      print("----------------")








