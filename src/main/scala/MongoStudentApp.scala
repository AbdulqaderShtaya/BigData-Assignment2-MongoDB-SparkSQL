import com.mongodb.client.{MongoClients, MongoCollection}
import com.mongodb.client.model.{Filters, Indexes, Projections, Sorts}
import org.bson.Document

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{LocalDate, ZoneOffset}
import java.util.Date
import scala.io.{Source, StdIn}
import scala.jdk.CollectionConverters._

/** Parts 1-3: CSV import, MongoDB indexes, and interactive queries, entirely in Scala. */
object MongoStudentApp {
  private val connectionString = "mongodb://localhost:27017"

  private def validCourse(text: String): Option[Int] =
    text.trim.toIntOption.filter(_ > 0)

  private def validGpa(text: String): Option[Double] =
    text.trim.toDoubleOption.filter(x => java.lang.Double.isFinite(x) && x >= 0.0 && x <= 4.0)

  private def readRequired[A](message: String, parse: String => Option[A]): A = {
    var result: Option[A] = None
    while (result.isEmpty) {
      result = parse(StdIn.readLine(message))
      if (result.isEmpty) println("Invalid input. Please try again.")
    }
    result.get
  }

  private def readOptional[A](message: String, parse: String => Option[A]): Option[A] = {
    var finished = false
    var result: Option[A] = None
    while (!finished) {
      val input = StdIn.readLine(message).trim
      if (input.isEmpty) finished = true
      else {
        result = parse(input)
        if (result.isDefined) finished = true
        else println("Invalid input. Please try again.")
      }
    }
    result
  }

  private def parseRow(line: String): (Document, Document) = {
    // CourseIDs includes commas; split only the first four delimiters.
    val firstFields = line.trim.split(",", 5)
    require(firstFields.length == 5, s"Invalid CSV row: $line")
    val lastComma = firstFields(4).lastIndexOf(',')
    require(lastComma > 0, s"Missing GPA in CSV row: $line")

    val id = firstFields(0).trim.toInt
    val firstName = firstFields(1).trim
    val lastName = firstFields(2).trim
    val birthDate = LocalDate.parse(firstFields(3).trim)
    val courseText = firstFields(4).substring(0, lastComma).trim
      .stripPrefix("[").stripSuffix("]")
    val courseIds = courseText.split(",").map(_.trim.toInt).toVector
    val gpa = firstFields(4).substring(lastComma + 1).trim.toDouble

    val mongoDate = Date.from(birthDate.atStartOfDay(ZoneOffset.UTC).toInstant)
    val mongoDocument = new Document("_id", Int.box(id))
      .append("firstName", firstName)
      .append("lastName", lastName)
      .append("dateOfBirth", mongoDate)
      .append("courseIds", courseIds.map(Int.box).asJava)
      .append("gpa", Double.box(gpa))

    // Use a plain timestamp string for spark.read.json (Option B).
    val jsonDocument = new Document("_id", Int.box(id))
      .append("firstName", firstName)
      .append("lastName", lastName)
      .append("dateOfBirth", birthDate.toString + "T00:00:00Z")
      .append("courseIds", courseIds.map(Int.box).asJava)
      .append("gpa", Double.box(gpa))

    (mongoDocument, jsonDocument)
  }

  private def importStudents(collection: MongoCollection[Document]): Unit = {
    val source = Source.fromFile("hw2-data.csv", "UTF-8")
    val rows = try {
      source.getLines().drop(1).filter(_.trim.nonEmpty).map(parseRow).toVector
    } finally source.close()

    collection.drop() // Safe to rerun: start with a fresh collection.
    collection.insertMany(rows.map(_._1).asJava)
    collection.createIndex(Indexes.ascending("courseIds")) // multikey index
    collection.createIndex(Indexes.ascending("gpa"))

    Files.write(
      Paths.get("students.json"),
      rows.map(_._2.toJson()).asJava,
      StandardCharsets.UTF_8
    )

    println(s"Inserted documents: ${collection.countDocuments()}")
    println("Indexes:")
    val indexes = collection.listIndexes().iterator()
    try indexes.asScala.foreach(index => println(s"  ${index.getString("name")}: ${index.get("key")}"))
    finally indexes.close()
    println("JSON Lines exported to students.json")
  }

  private def printStudents(
      collection: MongoCollection[Document],
      filter: org.bson.conversions.Bson,
      sort: org.bson.conversions.Bson
  ): Unit = {
    val iterator = collection.find(filter)
      .projection(Projections.include("_id", "firstName", "lastName", "gpa", "courseIds"))
      .sort(sort)
      .iterator()
    var count = 0
    try {
      iterator.asScala.foreach { student =>
        println(s"ID: ${student.getInteger("_id")} | Name: ${student.getString("firstName")} ${student.getString("lastName")} | GPA: ${student.getDouble("gpa")}")
        count += 1
      }
    } finally iterator.close()
    println(s"Total matches: $count")
  }

  private def courseQuery(collection: MongoCollection[Document], courseId: Int): Unit = {
    println(s"\n===== STUDENTS IN COURSE $courseId =====")
    val filter = Filters.eq("courseIds", Int.box(courseId))
    printStudents(collection, filter, Sorts.orderBy(Sorts.ascending("lastName"), Sorts.ascending("_id")))
    val explanation = collection.find(filter).explain()
    val planText = explanation.toJson()
    println("Query execution plan: " + planText)
    println("courseIds index IXSCAN used: " + planText.contains("IXSCAN"))
  }

  private def gpaQuery(collection: MongoCollection[Document], minGpa: Double): Unit = {
    println(s"\n===== STUDENTS WITH GPA > $minGpa =====")
    printStudents(collection, Filters.gt("gpa", Double.box(minGpa)), Sorts.descending("gpa"))
  }

  private def combinedQuery(
      collection: MongoCollection[Document],
      courseId: Option[Int],
      minGpa: Option[Double]
  ): Unit = {
    println("\n===== COMBINED COURSE / GPA QUERY =====")
    if (courseId.isEmpty && minGpa.isEmpty) {
      println("Please supply at least one criterion.")
      return
    }
    val conditions = Seq(
      courseId.map(id => Filters.eq("courseIds", Int.box(id))),
      minGpa.map(gpa => Filters.gt("gpa", Double.box(gpa)))
    ).flatten
    val filter = if (conditions.size == 1) conditions.head else Filters.and(conditions.asJava)
    printStudents(collection, filter,
      Sorts.orderBy(Sorts.descending("gpa"), Sorts.ascending("lastName"), Sorts.ascending("_id")))
  }

  def main(args: Array[String]): Unit = {
    val client = MongoClients.create(connectionString)
    try {
      val collection = client.getDatabase("university").getCollection("students")
      args.headOption.map(_.toLowerCase) match {
        case Some("import") => importStudents(collection)
        case Some("course") =>
          val id = args.lift(1).flatMap(validCourse)
            .getOrElse(readRequired("Enter Course ID: ", validCourse))
          courseQuery(collection, id)
        case Some("gpa") =>
          val threshold = args.lift(1).flatMap(validGpa)
            .getOrElse(readRequired("Enter minimum GPA (0.0 - 4.0): ", validGpa))
          gpaQuery(collection, threshold)
        case Some("combined") =>
          val course = if (args.length > 1) args(1).toIntOption.filter(_ > 0)
            else readOptional("Enter Course ID (Enter to skip): ", validCourse)
          val gpa = if (args.length > 2) validGpa(args(2))
            else readOptional("Enter minimum GPA (Enter to skip): ", validGpa)
          combinedQuery(collection, course, gpa)
        case Some("demo") =>
          importStudents(collection)
          courseQuery(collection, 103)
          gpaQuery(collection, 3.5)
          combinedQuery(collection, Some(103), Some(3.5))
        case _ =>
          println("Usage: runMain MongoStudentApp <import|course|gpa|combined|demo> [courseId] [minimumGpa]")
          println("Examples: runMain MongoStudentApp demo")
          println("          runMain MongoStudentApp course 103")
          println("          runMain MongoStudentApp gpa 3.5")
          println("          runMain MongoStudentApp combined 103 3.5")
      }
    } finally client.close()
  }
}
