import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

// Mirrors ../go/main.go field-for-field and benchmark-for-benchmark so the
// two programs' timings are comparable. See ../README.md.

final case class Icd10Row(
    ord: Int,
    code: String,
    id: Int,
    header: String,
    shortDesc: String,
    medDesc: String,
    longDesc: String,
    effectiveDate: java.sql.Date,
    tbl: String,
    codeWithPeriod: String)

final class Icd10Table(tag: Tag) extends Table[Icd10Row](tag, "icd10") {
  def ord = column[Int]("ord")
  def code = column[String]("code")
  def id = column[Int]("id")
  def header = column[String]("header")
  def shortDesc = column[String]("short_desc")
  def medDesc = column[String]("med_desc")
  def longDesc = column[String]("long_desc")
  def effectiveDate = column[java.sql.Date]("effective_date")
  def tbl = column[String]("tbl")
  def codeWithPeriod = column[String]("code_with_period")

  def * = (ord, code, id, header, shortDesc, medDesc, longDesc, effectiveDate, tbl, codeWithPeriod) <> (Icd10Row.tupled, Icd10Row.unapply)
}

final case class ScratchRow(id: Int, code: String, val1: Int, val2: String)

final class ScratchTable(tag: Tag) extends Table[ScratchRow](tag, "bench_squealx_slick_scratch") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def code = column[String]("code")
  def val1 = column[Int]("val1")
  def val2 = column[String]("val2")

  def * = (id, code, val1, val2) <> (ScratchRow.tupled, ScratchRow.unapply)
}

object Main extends App {
  def envOr(name: String, default: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(default)

  def envIntOr(name: String, default: Int): Int =
    sys.env.get(name).flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(default)

  val jdbcUrl = envOr(
    "BENCH_MYSQL_JDBC_URL",
    "jdbc:mysql://127.0.0.1:3306/cleardb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC")
  val dbUser = envOr("BENCH_MYSQL_USER", "service")
  val dbPass = envOr("BENCH_MYSQL_PASSWORD", "service")

  val pointLookups = envIntOr("BENCH_POINT_LOOKUPS", 2000)
  val rangeQueries = envIntOr("BENCH_RANGE_QUERIES", 1000)
  val bulkIterations = envIntOr("BENCH_BULK_ITERATIONS", 5)
  val bulkLimit = envIntOr("BENCH_BULK_LIMIT", 20000)
  val writeRows = envIntOr("BENCH_WRITE_ROWS", 2000)

  // Same pool shape as clear/fulltext's MySQLAdapter: AsyncExecutor bounds
  // both the thread pool and the JDBC connection count together.
  val db = Database.forURL(
    url = jdbcUrl,
    user = dbUser,
    password = dbPass,
    driver = "com.mysql.cj.jdbc.Driver",
    executor = AsyncExecutor("bench", minThreads = 10, maxThreads = 10, queueSize = 1000, maxConnections = 10))

  val icd10 = TableQuery[Icd10Table]
  val scratch = TableQuery[ScratchTable]

  def await[T](action: DBIO[T]): T = Await.result(db.run(action), Duration.Inf)

  def report(name: String, n: Int, elapsedNanos: Long): Unit = {
    val elapsedMs = elapsedNanos / 1e6
    val perOpMs = elapsedMs / n
    val opsPerSec = n / (elapsedNanos / 1e9)
    println(f"$name%-28s n=$n%-7d total=${elapsedMs}%.1fms avg=${perOpMs}%.3fms ops/sec=$opsPerSec%.1f")
  }

  def timed[T](f: => T): (T, Long) = {
    val start = System.nanoTime()
    val result = f
    (result, System.nanoTime() - start)
  }

  def runPointLookup(): Unit = {
    val sampleN = math.max(pointLookups, 200)
    val codes = await(
      sql"SELECT code FROM icd10 WHERE tbl = 'CM' ORDER BY RAND() LIMIT #$sampleN".as[String]).toVector

    val (_, elapsed) = timed {
      var i = 0
      while (i < pointLookups) {
        val code = codes(i % codes.length)
        await(icd10.filter(r => r.code === code).take(1).result)
        i += 1
      }
    }
    report("PointLookupByCode", pointLookups, elapsed)
  }

  def runIndexedRange(): Unit = {
    val headers = Vector("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T")
    val (_, elapsed) = timed {
      var i = 0
      while (i < rangeQueries) {
        val h = headers(i % headers.length)
        await(icd10.filter(r => r.header === h && r.tbl === "CM").take(100).result)
        i += 1
      }
    }
    report("IndexedRangeQuery(limit100)", rangeQueries, elapsed)
  }

  def runBulkStructScan(): Unit = {
    var totalRows = 0L
    val (_, elapsed) = timed {
      var i = 0
      while (i < bulkIterations) {
        val rows = await(icd10.filter(_.tbl === "CM").take(bulkLimit).result)
        totalRows += rows.size
        i += 1
      }
    }
    val elapsedMs = elapsed / 1e6
    val rowsPerSec = totalRows / (elapsed / 1e9)
    println(f"BulkStructScan             n=$bulkIterations%-7d total=${elapsedMs}%.1fms avg=${elapsedMs / bulkIterations}%.1fms rows/sec=$rowsPerSec%.0f ($totalRows rows)")
  }

  def runWriteScratch(): Unit = {
    await(sqlu"DROP TABLE IF EXISTS bench_squealx_slick_scratch")
    await(sqlu"""CREATE TABLE bench_squealx_slick_scratch (
      id INT PRIMARY KEY AUTO_INCREMENT,
      code VARCHAR(16) NOT NULL,
      val1 INT NOT NULL,
      val2 VARCHAR(64) NOT NULL
    ) ENGINE=InnoDB""")

    try {
      val rng = new Random(1)

      val (_, insertElapsed) = timed {
        var i = 0
        while (i < writeRows) {
          await(scratch.map(r => (r.code, r.val1, r.val2)) += ((f"C$i%06d", rng.nextInt(1000000), s"val-$i")))
          i += 1
        }
      }
      report("WriteScratch/Insert", writeRows, insertElapsed)

      val (_, updateElapsed) = timed {
        var i = 1
        while (i <= writeRows) {
          await(scratch.filter(_.id === i).map(r => (r.val1, r.val2)).update((rng.nextInt(1000000), s"updated-$i")))
          i += 1
        }
      }
      report("WriteScratch/Update", writeRows, updateElapsed)

      val count = await(scratch.length.result)
      require(count == writeRows, s"expected $writeRows scratch rows, got $count")
    } finally {
      await(sqlu"DROP TABLE IF EXISTS bench_squealx_slick_scratch")
    }
  }

  try {
    println("slick benchmark")
    println("================")
    runPointLookup()
    runIndexedRange()
    runBulkStructScan()
    runWriteScratch()
  } finally {
    db.close()
  }
}
