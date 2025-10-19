# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Telematics Streaming Pipeline
# MAGIC 
# MAGIC Processes IoT telematics data in real-time for usage-based insurance (UBI):
# MAGIC - Real-time driving behavior scoring
# MAGIC - Risk event detection (hard braking, speeding, etc.)
# MAGIC - Dynamic premium adjustments
# MAGIC - Instant driver feedback
# MAGIC - Fleet management insights

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# Configuration
CATALOG = "insurance_catalog"
BRONZE = "insurance_bronze"
GOLD = "insurance_gold"
CHECKPOINT_PATH = "/tmp/streaming_checkpoints/telematics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Telematics Data Schema

# COMMAND ----------

telematics_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("policy_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_mph", DoubleType(), True),
    StructField("acceleration_g", DoubleType(), True),
    StructField("braking_force_g", DoubleType(), True),
    StructField("steering_angle", DoubleType(), True),
    StructField("engine_rpm", IntegerType(), True),
    StructField("fuel_level_pct", DoubleType(), True),
    StructField("mileage_total", IntegerType(), True),
    StructField("trip_id", StringType(), True),
    StructField("is_night_driving", BooleanType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("road_type", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Telematics Processing

# COMMAND ----------

def create_telematics_stream():
    """
    Real-time telematics data processing with risk scoring
    """
    
    print("🔄 Starting real-time telematics pipeline...")
    
    # Read streaming telematics data
    # In production, this would come from Kafka/IoT Hub
    telematics_stream = (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .option("maxFilesPerTrigger", 1000)  # High throughput for IoT data
            .table(f"{CATALOG}.{BRONZE}.telematics_raw")
    )
    
    # Filter only new data
    new_data = telematics_stream.filter(
        col("_change_type") == "insert"
    )
    
    # RISK INDICATOR 1: Speeding Detection
    with_speeding = new_data.withColumn(
        "speed_limit_mph",
        when(col("road_type") == "HIGHWAY", 70)
        .when(col("road_type") == "URBAN", 35)
        .when(col("road_type") == "RESIDENTIAL", 25)
        .otherwise(55)
    ).withColumn(
        "is_speeding",
        col("speed_mph") > col("speed_limit_mph")
    ).withColumn(
        "speed_excess_mph",
        greatest(col("speed_mph") - col("speed_limit_mph"), lit(0))
    ).withColumn(
        "risk_ind_speeding",
        when(col("speed_excess_mph") > 30, 5)
        .when(col("speed_excess_mph") > 20, 4)
        .when(col("speed_excess_mph") > 15, 3)
        .when(col("speed_excess_mph") > 10, 2)
        .when(col("speed_excess_mph") > 5, 1)
        .otherwise(0)
    )
    
    # RISK INDICATOR 2: Hard Braking
    with_braking = with_speeding.withColumn(
        "is_hard_braking",
        col("braking_force_g") < -0.5  # Negative G-force indicates braking
    ).withColumn(
        "risk_ind_hard_braking",
        when(col("braking_force_g") < -1.0, 5)
        .when(col("braking_force_g") < -0.8, 4)
        .when(col("braking_force_g") < -0.6, 3)
        .when(col("braking_force_g") < -0.5, 2)
        .otherwise(0)
    )
    
    # RISK INDICATOR 3: Rapid Acceleration
    with_acceleration = with_braking.withColumn(
        "is_rapid_acceleration",
        col("acceleration_g") > 0.4
    ).withColumn(
        "risk_ind_acceleration",
        when(col("acceleration_g") > 0.8, 5)
        .when(col("acceleration_g") > 0.6, 4)
        .when(col("acceleration_g") > 0.5, 3)
        .when(col("acceleration_g") > 0.4, 2)
        .otherwise(0)
    )
    
    # RISK INDICATOR 4: Sharp Turns
    with_steering = with_acceleration.withColumn(
        "is_sharp_turn",
        abs(col("steering_angle")) > 45
    ).withColumn(
        "risk_ind_sharp_turn",
        when(abs(col("steering_angle")) > 90, 5)
        .when(abs(col("steering_angle")) > 70, 3)
        .when(abs(col("steering_angle")) > 45, 2)
        .otherwise(0)
    )
    
    # RISK INDICATOR 5: High RPM (Engine Stress)
    with_rpm = with_steering.withColumn(
        "is_high_rpm",
        col("engine_rpm") > 5000
    ).withColumn(
        "risk_ind_high_rpm",
        when(col("engine_rpm") > 7000, 4)
        .when(col("engine_rpm") > 6000, 3)
        .when(col("engine_rpm") > 5000, 2)
        .otherwise(0)
    )
    
    # RISK INDICATOR 6: Night Driving
    with_night = with_rpm.withColumn(
        "risk_ind_night_driving",
        when((col("is_night_driving") == True) & (col("speed_mph") > 60), 3)
        .when(col("is_night_driving") == True, 1)
        .otherwise(0)
    )
    
    # RISK INDICATOR 7: Adverse Weather
    adverse_weather = ['RAIN', 'SNOW', 'FOG', 'ICE']
    with_weather = with_night.withColumn(
        "risk_ind_weather",
        when((col("weather_condition").isin(adverse_weather)) & (col("speed_mph") > 50), 4)
        .when(col("weather_condition").isin(adverse_weather), 2)
        .otherwise(0)
    )
    
    # CALCULATE OVERALL DRIVING RISK SCORE
    risk_scored = with_weather.withColumn(
        "risk_indicators_sum",
        col("risk_ind_speeding") +
        col("risk_ind_hard_braking") +
        col("risk_ind_acceleration") +
        col("risk_ind_sharp_turn") +
        col("risk_ind_high_rpm") +
        col("risk_ind_night_driving") +
        col("risk_ind_weather")
    ).withColumn(
        "driving_risk_score",
        least((col("risk_indicators_sum") / 28.0 * 100).cast("int"), lit(100))
    ).withColumn(
        "driving_risk_category",
        when(col("driving_risk_score") >= 80, "DANGEROUS")
        .when(col("driving_risk_score") >= 60, "RISKY")
        .when(col("driving_risk_score") >= 40, "MODERATE")
        .when(col("driving_risk_score") >= 20, "CAUTIOUS")
        .otherwise("SAFE")
    ).withColumn(
        "requires_alert",
        (col("driving_risk_category").isin(["DANGEROUS", "RISKY"])) |
        (col("is_speeding") & (col("speed_excess_mph") > 20)) |
        (col("is_hard_braking") & (col("speed_mph") > 60))
    )
    
    # Calculate trip-level aggregations using windowing
    window_spec = Window.partitionBy("trip_id").orderBy(col("timestamp"))
    
    trip_aggregated = risk_scored.withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("timestamp")) - 
         unix_timestamp(first(col("timestamp")).over(window_spec))) / 60
    ).withColumn(
        "trip_distance_miles",
        (col("mileage_total") - first(col("mileage_total")).over(window_spec))
    )
    
    # DYNAMIC PREMIUM CALCULATION
    with_premium = trip_aggregated.withColumn(
        "behavior_premium_factor",
        when(col("driving_risk_category") == "SAFE", 0.85)  # 15% discount
        .when(col("driving_risk_category") == "CAUTIOUS", 0.95)  # 5% discount
        .when(col("driving_risk_category") == "MODERATE", 1.0)  # No change
        .when(col("driving_risk_category") == "RISKY", 1.15)  # 15% increase
        .when(col("driving_risk_category") == "DANGEROUS", 1.30)  # 30% increase
        .otherwise(1.0)
    ).withColumn(
        "recommended_action",
        when(col("driving_risk_category") == "DANGEROUS", "IMMEDIATE_ALERT_DRIVER")
        .when((col("is_speeding")) & (col("speed_excess_mph") > 25), "SPEEDING_WARNING")
        .when(col("is_hard_braking") & (col("speed_mph") > 60), "UNSAFE_BRAKING_ALERT")
        .when(col("driving_risk_category") == "RISKY", "COACHING_RECOMMENDED")
        .otherwise("NO_ACTION")
    )
    
    # Add processing metadata
    final_stream = with_premium.withColumn(
        "processed_timestamp",
        current_timestamp()
    ).withColumn(
        "processing_latency_ms",
        (unix_timestamp(current_timestamp()) - unix_timestamp(col("timestamp"))) * 1000
    )
    
    # Select final columns
    telematics_enriched = final_stream.select(
        col("device_id"),
        col("policy_id"),
        col("customer_id"),
        col("vehicle_id"),
        col("timestamp"),
        col("trip_id"),
        col("latitude"),
        col("longitude"),
        col("speed_mph"),
        col("speed_limit_mph"),
        col("speed_excess_mph"),
        col("acceleration_g"),
        col("braking_force_g"),
        col("steering_angle"),
        col("engine_rpm"),
        col("weather_condition"),
        col("road_type"),
        col("is_night_driving"),
        col("is_speeding"),
        col("is_hard_braking"),
        col("is_rapid_acceleration"),
        col("is_sharp_turn"),
        col("driving_risk_score"),
        col("driving_risk_category"),
        col("risk_ind_speeding"),
        col("risk_ind_hard_braking"),
        col("risk_ind_acceleration"),
        col("risk_ind_sharp_turn"),
        col("risk_ind_high_rpm"),
        col("risk_ind_night_driving"),
        col("risk_ind_weather"),
        col("trip_duration_minutes"),
        col("trip_distance_miles"),
        col("behavior_premium_factor"),
        col("requires_alert"),
        col("recommended_action"),
        col("processed_timestamp"),
        col("processing_latency_ms")
    )
    
    return telematics_enriched

# Create the telematics stream
telematics_stream = create_telematics_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Telematics Data to Gold Layer

# COMMAND ----------

# Write ALL telematics events
all_events_query = (
    telematics_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/all_events")
        .partitionBy("customer_id")
        .trigger(processingTime="5 seconds")
        .table(f"{CATALOG}.{GOLD}.telematics_enriched")
)

print(f"✓ Started telematics stream: {all_events_query.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write High-Risk Events for Alerts

# COMMAND ----------

# Filter and write only high-risk events
high_risk_events = telematics_stream.filter(
    col("requires_alert") == True
)

alerts_query = (
    high_risk_events.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/risk_alerts")
        .trigger(processingTime="1 second")  # Faster for alerts
        .table(f"{CATALOG}.{GOLD}.telematics_risk_alerts")
)

print(f"✓ Started telematics risk alerts stream: {alerts_query.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Dashboard Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Real-time driving behavior statistics
# MAGIC SELECT 
# MAGIC     driving_risk_category,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COUNT(DISTINCT customer_id) as unique_drivers,
# MAGIC     AVG(driving_risk_score) as avg_risk_score,
# MAGIC     AVG(speed_mph) as avg_speed,
# MAGIC     SUM(CASE WHEN is_speeding THEN 1 ELSE 0 END) as speeding_events,
# MAGIC     SUM(CASE WHEN is_hard_braking THEN 1 ELSE 0 END) as hard_braking_events,
# MAGIC     AVG(behavior_premium_factor) as avg_premium_factor,
# MAGIC     AVG(processing_latency_ms) as avg_latency_ms
# MAGIC FROM insurance_catalog.insurance_gold.telematics_enriched
# MAGIC WHERE processed_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
# MAGIC GROUP BY driving_risk_category
# MAGIC ORDER BY avg_risk_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Streaming Performance

# COMMAND ----------

def monitor_telematics_streams():
    """Monitor telematics streaming performance"""
    
    print("\n📊 Telematics Streaming Metrics:")
    print("=" * 80)
    
    for stream in spark.streams.active:
        if 'telematics' in stream.name.lower():
            print(f"\n🔍 Stream: {stream.name}")
            print(f"   Active: {stream.isActive}")
            
            if stream.lastProgress:
                progress = stream.lastProgress
                print(f"   Input Rows: {progress.get('numInputRows', 0):,}")
                print(f"   Processing Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
    
    print("=" * 80)

# Monitor streams
monitor_telematics_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benefits of Real-Time Telematics
# MAGIC 
# MAGIC ✅ **Usage-Based Insurance (UBI)**: Pay-per-mile and behavior-based pricing  
# MAGIC ✅ **Real-Time Safety**: Instant driver alerts for dangerous behavior  
# MAGIC ✅ **Dynamic Premiums**: Automatic premium adjustments based on driving  
# MAGIC ✅ **Fleet Management**: Real-time vehicle tracking and driver monitoring  
# MAGIC ✅ **Accident Prevention**: Proactive risk identification  
# MAGIC ✅ **Customer Engagement**: Gamification and driver coaching  
# MAGIC ✅ **Claims Verification**: IoT data validates incident reports  

# COMMAND ----------

print("✅ Real-time telematics pipeline is active!")
print("⚡ Processing IoT data from vehicles in real-time...")
print(f"📊 Processing latency: < 5 seconds")
print(f"🚗 Enabling usage-based insurance and driver safety monitoring")

