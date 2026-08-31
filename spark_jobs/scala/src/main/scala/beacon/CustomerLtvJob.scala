package beacon

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Beacon GTM Lakehouse — Customer Lifetime Value job (Scala).
 *
 * This is the one deliberately-Scala job in the project (everything else
 * is PySpark). It mirrors the customer_activity gold table but adds a
 * daily revenue rollup + a simple LTV projection, since this is a good,
 * self-contained example to show Scala/Spark proficiency without
 * duplicating the whole pipeline in a second language.
 *
 * Run (after building a fat jar with sbt assembly):
 *   spark-submit --class beacon.CustomerLtvJob beacon-scala-jobs.jar \
 *       --silver ../../data/silver --gold ../../data/gold
 */
object CustomerLtvJob {

  def main(args: Array[String]): Unit = {
    val argMap = args.grouped(2).collect { case Array(k, v) => k.stripPrefix("--") -> v }.toMap
    val silverPath = argMap.getOrElse("silver", "../../data/silver")
    val goldPath = argMap.getOrElse("gold", "../../data/gold")

    val spark = SparkSession.builder()
      .appName("beacon-customer-ltv-scala")
      .getOrCreate()

    import spark.implicits._

    val events = spark.read.parquet(s"$silverPath/fact_event")

    // Daily revenue per customer
    val dailyRevenue = events
      .withColumn("event_date", F.to_date($"event_timestamp"))
      .groupBy($"customer_id", $"event_date")
      .agg(F.sum($"revenue_usd").as("daily_revenue_usd"))

    // Customer-level rollup: total revenue, active days, avg daily revenue
    val customerRollup = dailyRevenue
      .groupBy($"customer_id")
      .agg(
        F.sum($"daily_revenue_usd").as("total_revenue_usd"),
        F.countDistinct($"event_date").as("active_days"),
        F.avg($"daily_revenue_usd").as("avg_daily_revenue_usd")
      )

    // Naive LTV projection: avg daily revenue * a 365-day horizon.
    // (Simple on purpose — the point is showing the Spark/Scala mechanics,
    // not a sophisticated forecasting model. Swap in a proper survival/
    // cohort model if you want to extend this.)
    val withLtv = customerRollup
      .withColumn("projected_annual_ltv_usd", F.round($"avg_daily_revenue_usd" * F.lit(365), 2))
      .orderBy(F.desc("projected_annual_ltv_usd"))

    withLtv.write.mode("overwrite").parquet(s"$goldPath/customer_ltv_scala")

    println(s"Wrote ${withLtv.count()} customer LTV rows to $goldPath/customer_ltv_scala")

    spark.stop()
  }
}
