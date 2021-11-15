import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager}


object hiveP1 {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("Creating spark session...")
    spark.sparkContext.setLogLevel("ERROR")

    showMenu()

    //Prints opening messages. Asks for user to login to begin.
    def showMenu() {
      println("Hello! Welcome to my Project One!")
      println("Login with your username and password to get started.")
      println("Please enter your username: ")
      val username = scala.io.StdIn.readLine()
      println("Please enter your password: ")
      val password = scala.io.StdIn.readLine()

      if(checkLogin(username, password)) {
        println("Welcome Back.")
        checkOptions(username, password)

      }
      else {
        println("Username or password incorrect. Please try again.")

        println("Please enter your username: ")
        val username = scala.io.StdIn.readLine()
        println("Please enter your password: ")
        val password = scala.io.StdIn.readLine()

        if(checkLogin(username, password)) {
          println("Welcome Back.")
          checkOptions(username, password)

        }
      }
    }

    //Checks login information against the database.
    def checkLogin(username: String, password: String): Boolean = {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = ""

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM users WHERE username = '"+username+"' AND password = '"+password+"'")
      resultSet.next()
    }

    //If login info is present, user will be given the option to update or continue.
    //If the user wishes to update, prompt user for new login info and update MySQL database.
    def checkOptions(username: String, password: String) {
      println("Type 'Update' to update your information or 'Continue' to proceed.")
      val infoCheck = scala.io.StdIn.readLine()
      if (infoCheck == "Update") {
        println("What would you like your new username to be? ")
        val newUser = scala.io.StdIn.readLine()
        println("What would you like your new password to be? ")
        val newPass = scala.io.StdIn.readLine()
        updateLogin(username, password, newUser, newPass)
        println("Changes saved.")
        checkPermissions(newUser, newPass)
      }
      else{
        checkPermissions(username, password)
      }
    }

    //Updates MySQL database with new login information.
    def updateLogin(username: String, password: String, newUser: String, newPass: String): Unit = {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = ""
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)

      val Statement = connection.createStatement()
      val resultSet = Statement.executeQuery("SELECT * FROM users WHERE username = '"+username+"' AND password = '"+password+"'")
      resultSet.next()
      val id = resultSet.getInt("id")

      val statement = connection.createStatement()
      val sql = statement.executeUpdate("UPDATE users SET username = '"+newUser+"' , password = '"+newPass+"' WHERE id = "+id+"")
    }

    //Checks assigned permissions based on login information given by user.
    //Basic users will be allowed to see the first question and data result.
    //Admin users will be allowed to choose what question and data results they wish to view.
    def checkPermissions(username: String, password: String){
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = ""

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM users WHERE username = '"+username+"' AND password = '"+password+"'")
      while ( resultSet.next() ) {
        val admin = resultSet.getInt("adminFlag")
        if (admin == 0) {
          println("Logged in as BASIC user, you will be given access to two questions and the results.")
          println("How many movies were made in 2015?")
          println("Loading results now...")
          problem1Scenario()
          println("What is the average rating for a movie in 2012?")
          println("Loading results now...")
          problem2Scenario()
        }
        else {
          println("Logged in as ADMIN user, you will be given full access to the questions")
          chooseQuestion()
          reRunMenu()
        }
      }
    }

    //Prompt Admin user for what question and data result they would like to view.
    def chooseQuestion(){
      //Receive the input from the user.
      println("What results would you like to see? example: Problem 1 ")
      println("Please type in an option: ")
      val actionInput = scala.io.StdIn.readLine()

      while (actionInput != "Exit") {
        if (actionInput == "Problem 1") {
          println("Problem 1: What is the number of movies made in 2015? ")
          println("Loading the results now...")

          return problem1Scenario()
        }
        else if (actionInput == "Problem 2") {
          println("Problem 2: What is the average rating for a movie in 2012?")
          println("Loading the results now...")

          return problem2Scenario()
        }
        else if (actionInput == "Problem 3") {
          println("Problem 3: What are the highest grossing movies in 2010?")
          println("Loading the results now...")

          return problem3Scenario()
        }
        else if (actionInput == "Problem 4") {
          println("Problem 4: What movies have been directed by Christopher Nolan?")
          println("Loading the results now...")

          return problem4Scenario()
        }
        else if (actionInput == "Problem 5") {
          println("Problem 5: Which movie has the longest runtime in 2005?")
          println("Loading the results now...")

          return problem5Scenario()
        }
        else if (actionInput == "Problem 6") {
          println("Problem 6 Future Trend: Are movies becoming shorter?")
          println("Loading the results now...")

          return problem6Scenario()
        }
        else {
          println("Thank you for coming.")
        }
      }
    }

    //Asks user if they would like to view another question.
    def reRunMenu(): Unit = {
      println("Would you like to view another question?")
      println("Type 'yes' to return to choose another question or 'no' to exit. ")
      val answer = scala.io.StdIn.readLine()

      if(answer == "yes"){
        chooseQuestion()
        reRunMenu()
      }
      else {
        println("Goodbye!")
      }
    }

    //Problem #1
    //What is the number of movies made in 2015?
    //Sum of movies made in 2015
    def problem1Scenario(): Unit =  {
      spark.sql("DROP table IF EXISTS movieDatabase1")
      spark.sql("CREATE TABLE IF NOT EXISTS movieDatabase1(original_title varchar(255), director varchar(255), genres array<String>, runtime Int, release_year Int , " +
        "vote_average Double, budget varchar(255), revenue Int) row format delimited fields terminated by ',' collection items terminated by '|'")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/brade/IdeaProjects/Project1/Input/MovieDatabaseNew.csv' INTO TABLE movieDatabase1")
      spark.sql("SELECT sum(original_title) as Total_Movies_In_2015 FROM movieDatabase1 WHERE release_year = 2015").show()
    }

    //Problem #2
    //What is the average rating for a movie in 2012?
    //Find average rating in the year 2012
    def problem2Scenario(): Unit =  {
      spark.sql("DROP table IF EXISTS movieDatabase2")
      spark.sql("CREATE TABLE IF NOT EXISTS movieDatabase2(original_title varchar(255), director varchar(255), genres array<String>, runtime Int, release_year Int , " +
        "vote_average Double, budget varchar(255), revenue Int) row format delimited fields terminated by ',' collection items terminated by '|'")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/brade/IdeaProjects/Project1/Input/MovieDatabaseNew.csv' INTO TABLE movieDatabase2")
      spark.sql("SELECT round(avg(vote_average), 2) as Avg_Rating_In_2012, sum(original_title) as Total_Movies FROM movieDatabase2 WHERE release_year = 2012").show()
     }

    //Problem #3
    //What are the highest grossing movies in 2010?
    //Find highest revenue in 2010
    def problem3Scenario(): Unit = {
      spark.sql("DROP table IF EXISTS movieDatabase3")
      spark.sql("CREATE TABLE IF NOT EXISTS movieDatabase3(original_title varchar(255), director varchar(255), genres array<String>, runtime Int, release_year Int , " +
        "vote_average Double, budget varchar(255), revenue Int) row format delimited fields terminated by ',' collection items terminated by '|'")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/brade/IdeaProjects/Project1/Input/MovieDatabaseNew.csv' INTO TABLE movieDatabase3")
      spark.sql("SELECT original_title as Movie_Title, revenue as Revenue FROM movieDatabase3 WHERE release_year = 2010 ORDER BY revenue DESC").show()

    }

    //Problem #4
    //What movies have been directed by Christopher Nolan?
    def problem4Scenario(): Unit = {
      spark.sql("DROP table IF EXISTS movieDatabase4")
      spark.sql("CREATE TABLE IF NOT EXISTS movieDatabase4(original_title varchar(255), director varchar(255), genres array<String>, runtime Int, release_year Int , " +
        "vote_average Double, budget varchar(255), revenue Int) row format delimited fields terminated by ',' collection items terminated by '|'")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/brade/IdeaProjects/Project1/Input/MovieDatabaseNew.csv' INTO TABLE movieDatabase4")
      spark.sql("SELECT original_title as Movie_Title FROM movieDatabase4 WHERE director = 'Christopher Nolan'" ).show()
    }

    //Problem #5
    //What is the longest movie in 2005?
    //Find highest runtime in 2005
    def problem5Scenario(): Unit = {
      spark.sql("DROP table IF EXISTS movieDatabase5")
      spark.sql("CREATE TABLE IF NOT EXISTS movieDatabase5(original_title varchar(255), director varchar(255), genres array<String>, runtime Int, release_year Int , " +
        "vote_average Double, budget varchar(255), revenue Int) row format delimited fields terminated by ',' collection items terminated by '|'")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/brade/IdeaProjects/Project1/Input/MovieDatabaseNew.csv' INTO TABLE movieDatabase5")
      spark.sql("SELECT original_title as Movie_Title, runtime as Total_Runtime_In_Minutes FROM movieDatabase5 WHERE release_year = 2005 ORDER BY runtime DESC").show()
    }

    //Problem #6
    //Are films getting longer or shorter?
    //Column 1: movie title
    //Column 2: find the avg length of movies per year
    def problem6Scenario(): Unit = {
      spark.sql("DROP table IF EXISTS movieDatabase6")
      spark.sql("CREATE TABLE IF NOT EXISTS movieDatabase6(original_title varchar(255), director varchar(255), genres array<String>, runtime Int, release_year Int , " +
        "vote_average Double, budget varchar(255), revenue Int) row format delimited fields terminated by ',' collection items terminated by '|'")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/brade/IdeaProjects/Project1/Input/MovieDatabaseNew.csv' INTO TABLE movieDatabase6")
      spark.sql("SELECT release_year as Year, round(avg(runtime), 2) as Avg_Length_In_Minutes FROM movieDatabase6 GROUP BY release_year ORDER BY release_year DESC ").show(25)

      //Calculate estimated runtime in 2016
      spark.sql("SELECT round((96.27-0.53), 2) as Runtime_Prediction_In_2016 FROM movieDatabase6 ").show(1)
      println("We can see that movie runtimes have consistently shortened over the years.")
      println("This could be for a multitude of reasons from a decrease in viewer attention span to the increase in high production series driven shows.")
      println("Based on these results, we can predict movies in 2016 will have a runtime of 95.74")
    }
  }
}

