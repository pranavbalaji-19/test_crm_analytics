// =============================================================================
// Spark Job   : CustomerSegmentation.scala
// Package     : com.company.crm
// Description : Scala Spark job for advanced customer segmentation.
//               Reads from both CRM schema (FACT_CUSTOMER_SCORES) AND
//               Retail schema (DW_OWNER.STG_CUSTOMER_SALES,
//                             DW_OWNER.FACT_REGIONAL_SUMMARY)
//               to produce enriched cross-domain customer segments.
//               Demonstrates cross-repo data lineage in Spark.
// =============================================================================

package com.company.crm

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.util.Properties

object CustomerSegmentation {

  case class Config(
    runDate:      String = "",
    modelVersion: String = "v2.1",
    crmHost:      String = "",
    crmSid:       String = "",
    crmUser:      String = "",
    crmPass:      String = "",
    env:          String = "PROD"
  )

  def parseArgs(args: Array[String]): Config = {
    var cfg = Config()
    args.sliding(2, 2).foreach {
      case Array("--run-date",      v) => cfg = cfg.copy(runDate = v)
      case Array("--model-version", v) => cfg = cfg.copy(modelVersion = v)
      case Array("--env",           v) => cfg = cfg.copy(env = v)
      case _ =>
    }
    require(cfg.runDate.nonEmpty, "--run-date required")
    cfg
  }

  def buildJdbcUrl(host: String, port: String, sid: String): String =
    s"jdbc:oracle:thin:@$host:$port:$sid"

  def jdbcProps(user: String, pass: String): Properties = {
    val p = new Properties()
    p.put("user",     user)
    p.put("password", pass)
    p.put("driver",   "oracle.jdbc.OracleDriver")
    p.put("fetchsize", "10000")
    p
  }

  def readQuery(spark: SparkSession, url: String, props: Properties, sql: String): DataFrame =
    spark.read.jdbc(url, s"($sql)", props)

  // -------------------------------------------------------------------------
  // Read CRM customer scores
  // -------------------------------------------------------------------------
  def readCrmScores(spark: SparkSession, url: String, props: Properties,
                    runDate: String): DataFrame = {
    val q =
      s"""SELECT sc.customer_id,
         |       sc.dim_crm_customer_key,
         |       sc.clv_score,
         |       sc.churn_risk_score,
         |       sc.propensity_buy_score,
         |       sc.engagement_score,
         |       sc.segment_code,
         |       sc.segment_label,
         |       dc.region_code,
         |       dc.acquisition_channel,
         |       dc.is_opted_in_email
         |FROM   fact_customer_scores sc
         |JOIN   dim_customer_crm dc
         |       ON dc.customer_id = sc.customer_id AND dc.is_current = 'Y'
         |WHERE  sc.score_date = TO_DATE('$runDate','YYYY-MM-DD')
         """.stripMargin
    readQuery(spark, url, props, q)
  }

  // -------------------------------------------------------------------------
  // Read retail spend from DW_OWNER shared staging (cross-repo read)
  // -------------------------------------------------------------------------
  def readRetailSpend(spark: SparkSession, url: String, props: Properties): DataFrame = {
    val q =
      """SELECT customer_id,
        |       lifetime_value,
        |       loyalty_tier,
        |       last_purchase_date,
        |       region_code AS retail_region_code
        |FROM   DW_OWNER.STG_CUSTOMER_SALES
        """.stripMargin
    readQuery(spark, url, props, q)
  }

  // -------------------------------------------------------------------------
  // Read regional retail summary (cross-repo: retail_dw_legacy)
  // -------------------------------------------------------------------------
  def readRegionalSummary(spark: SparkSession, url: String, props: Properties,
                           runDate: String): DataFrame = {
    val q =
      s"""SELECT region_code,
         |       summary_date,
         |       total_revenue,
         |       distinct_customers,
         |       avg_basket_size
         |FROM   DW_OWNER.FACT_REGIONAL_SUMMARY
         |WHERE  summary_date = TO_DATE('$runDate','YYYY-MM-DD')
         """.stripMargin
    readQuery(spark, url, props, q)
  }

  // -------------------------------------------------------------------------
  // K-means style segmentation using Spark's built-in window functions
  // (Simplified: threshold-based segmentation without MLlib for legacy compat)
  // -------------------------------------------------------------------------
  def computeSegments(scoresDf: DataFrame, retailDf: DataFrame,
                      regionalDf: DataFrame): DataFrame = {

    // Join CRM scores with retail lifetime value
    val enriched = scoresDf.join(
      retailDf.select("customer_id","lifetime_value","loyalty_tier","last_purchase_date"),
      Seq("customer_id"), "left"
    ).join(
      regionalDf.select("region_code","total_revenue","avg_basket_size")
                .withColumnRenamed("region_code","reg_region"),
      col("region_code") === col("reg_region"), "left"
    ).drop("reg_region")

    // Compute composite segment score
    val scored = enriched
      .withColumn("composite_score",
        (col("clv_score")            * 0.30) +
        ((lit(1.0) - col("churn_risk_score")) * 0.25) +
        (col("engagement_score")     * 0.20) +
        (col("propensity_buy_score") * 0.15) +
        (least(coalesce(col("lifetime_value"), lit(0.0)) / lit(10000.0), lit(1.0)) * 0.10)
      )

    // Assign micro-segment
    val microsegmented = scored.withColumn("micro_segment",
      when(col("composite_score") >= 0.8,  "CHAMPION")
      .when(col("composite_score") >= 0.6,  "LOYAL_CUSTOMER")
      .when(col("composite_score") >= 0.4,  "POTENTIAL_LOYALIST")
      .when(col("composite_score") >= 0.2,  "AT_RISK")
      .otherwise("LOST_CUSTOMER")
    ).withColumn("intervention_recommended",
      when(col("churn_risk_score") > 0.7 && col("composite_score") >= 0.4, "WINBACK_CAMPAIGN")
      .when(col("churn_risk_score") > 0.5 && col("engagement_score") < 0.3, "REENGAGEMENT_CAMPAIGN")
      .when(col("clv_score") > 7.0 && col("is_opted_in_email") === "Y",    "VIP_UPGRADE")
      .otherwise("NO_ACTION")
    )

    microsegmented
  }

  // -------------------------------------------------------------------------
  // Compute region-level segment distribution
  // -------------------------------------------------------------------------
  def computeRegionSegmentDist(segmentedDf: DataFrame): DataFrame = {
    val regionWindow = Window.partitionBy("region_code")

    segmentedDf.groupBy("region_code", "micro_segment", "segment_code")
      .agg(
        count("*").alias("customer_count"),
        avg("clv_score").alias("avg_clv"),
        avg("churn_risk_score").alias("avg_churn_risk"),
        avg("composite_score").alias("avg_composite_score"),
        sum(when(col("is_opted_in_email") === "Y", 1).otherwise(0)).alias("email_opted_in"),
        avg(coalesce(col("lifetime_value"), lit(0.0))).alias("avg_retail_lifetime_value")
      )
      .withColumn("pct_of_region",
        col("customer_count").cast("double") /
        sum("customer_count").over(Window.partitionBy("region_code")) * 100
      )
  }

  // -------------------------------------------------------------------------
  // Main
  // -------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    val cfg = parseArgs(args)

    val crmHost = sys.env.getOrElse("CRM_DB_HOST", "oradb-crm-prod.internal.company.com")
    val crmPort = sys.env.getOrElse("CRM_DB_PORT", "1521")
    val crmSid  = sys.env.getOrElse("CRM_DB_SID",  "CRMDB")
    val crmUser = sys.env.getOrElse("CRM_DB_USER", "CRM_SCHEMA")
    val crmPass = sys.env.getOrElse("CRM_DB_PASS", "")

    val url   = buildJdbcUrl(crmHost, crmPort, crmSid)
    val props = jdbcProps(crmUser, crmPass)

    val spark = SparkSession.builder()
      .appName(s"CRMCustomerSegmentation-${cfg.runDate}")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.shuffle.partitions", "100")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println(s"[INFO] CustomerSegmentation: date=${cfg.runDate} model=${cfg.modelVersion}")

    // 1. Read CRM customer scores
    val scoresDf = readCrmScores(spark, url, props, cfg.runDate).cache()
    println(s"[INFO] CRM scores: ${scoresDf.count()} rows")

    // 2. Read retail spend (cross-repo: retail_dw_legacy via DW_OWNER grant)
    val retailDf = readRetailSpend(spark, url, props).cache()
    println(s"[INFO] Retail spend (cross-repo): ${retailDf.count()} rows")

    // 3. Read regional summary (cross-repo: retail_dw_legacy)
    val regionalDf = readRegionalSummary(spark, url, props, cfg.runDate).cache()
    println(s"[INFO] Regional summary (cross-repo): ${regionalDf.count()} rows")

    // 4. Compute segments
    val segmentedDf = computeSegments(scoresDf, retailDf, regionalDf).cache()
    println(s"[INFO] Segmented customers: ${segmentedDf.count()} rows")

    // 5. Region-level distribution
    val regionDistDf = computeRegionSegmentDist(segmentedDf)

    // 6. Write results
    segmentedDf.select(
      "customer_id","segment_code","micro_segment","composite_score",
      "churn_risk_score","clv_score","engagement_score",
      "intervention_recommended","region_code"
    ).write.mode(SaveMode.Append)
      .jdbc(url, "CRM_MICRO_SEGMENTS", props)

    regionDistDf.write.mode(SaveMode.Overwrite)
      .jdbc(url, "CRM_REGION_SEGMENT_DIST", props)

    // Update segment summary with composite scores
    segmentedDf.groupBy("segment_code","region_code")
      .agg(
        count("*").alias("customer_count"),
        avg("clv_score").alias("avg_clv_score"),
        avg("churn_risk_score").alias("avg_churn_risk"),
        sum(when(col("clv_score") > 5.0, 1).otherwise(0)).alias("high_value_count"),
        sum(when(col("churn_risk_score") > 0.7, 1).otherwise(0)).alias("at_risk_count"),
        avg(coalesce(col("lifetime_value"), lit(0.0))).alias("avg_retail_spend")
      )
      .write.mode(SaveMode.Append)
      .jdbc(url, "CRM_SEGMENT_WEEKLY_SNAPSHOT", props)

    println(s"[INFO] CustomerSegmentation complete for ${cfg.runDate}")
    spark.stop()
  }
}
