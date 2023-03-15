attr_stream_data = spark.readStream.format("cloudFiles") \
                        .option("cloudFiles.format", "csv") \
                        .option("cloudFiles.schemaLocation", 
                                "dbfs:/FileStore/datasets/attr_source_stream") \
                        .option("cloudFiles.schemaHints", 
                                """Age int, DailyRate int, DistanceFromHome int, 
                                   HourlyRate int, JobLevel int, JobSatisfaction int, MonthlyIncome int, 
                                   PercentSalaryHike int, PerformanceRating int, YearsSinceLastPromotion int, 
                                   YearsWithCurrManager int""")\
                        .load("dbfs:/FileStore/datasets/attr_source_stream")


attr_stream_subset = attr_stream_data.select("Age", "Gender", "BusinessTravel", "JobSatisfaction", "PerformanceRating")\
                                     .filter("PerformanceRating > 3")


attr_stream_subset.writeStream \
                  .option("mergeSchema", "true") \
                  .format("csv") \
                  .option("checkpointLocation", 
                          "dbfs:/FileStore/datasets/attr_dest_location/checkpoint_dir") \
                  .start("dbfs:/FileStore/datasets/attr_dest_location/")


