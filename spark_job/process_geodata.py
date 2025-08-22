import logging, boto3, json, sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

logging.basicConfig(level=logging.INFO) 


class GeospatialProcessor:
    """Class to process geospatial data from PostgreSQL and save results to S3"""
    
    def __init__(self, env):
        self.env = env
        self.parameter_name = f"/path_to_credentials/{env}/database/credentials"
        self.spark = self._create_spark_session()
        self.sedona = SedonaContext.create(self.spark)
    
    def _create_spark_session(self, app_name='geospatial_analysis_job'):
        """Create and return a Spark session"""
        spark_builder = SparkSession.builder.appName(app_name)
        spark_session = spark_builder.getOrCreate()
        logging.info('Spark Session created.')
        return spark_session
    
    def _get_parameter(self, region_name="us-east-1"):
        """Retrieve database credentials from AWS SSM Parameter Store"""
        try:
            ssm_client = boto3.client("ssm", region_name=region_name)     
            response = ssm_client.get_parameter(
                Name=self.parameter_name,
                WithDecryption=True  
            )
            
            parameter_value = response["Parameter"]["Value"]
            credentials = json.loads(parameter_value)
            
            required_keys = {"host", "user", "password"}
            if not required_keys.issubset(credentials.keys()):
                raise ValueError(f"The obtained JSON does not contain all required keys: {required_keys}")
            
            return credentials

        except NoCredentialsError:
            logging.error("Error: AWS credentials were not found.")
            raise
        except PartialCredentialsError:
            logging.error("Error: AWS credentials are incomplete.")
            raise
        except Exception as e:
            logging.error(f"Error fetching or interpreting the parameter: {str(e)}")
            raise
    
    def _read_data_bounds(self, table):
        """Read maximum and minimum values from a table for partitioning"""
        credentials = self._get_parameter(region_name="us-east-1")
        connection_str = "jdbc:postgresql://" + credentials["host"] + ":5432/postgres_db"
        
        properties = {       
            "user": credentials["user"],
            "password": credentials["password"],
            "driver": "org.postgresql.Driver"
        }

        df = self.spark.read.jdbc(
            url=connection_str,
            properties=properties,
            table=table
        )
        max_value = df.collect()[0][0]
        min_value = df.collect()[0][1]
        return max_value, min_value
    
    def _read_data_from_db(self, table, partition_column, upper_bound, lower_bound, num_partitions):
        """Read data from PostgreSQL with partitioning for better performance"""
        credentials = self._get_parameter(region_name="us-east-1")
        connection_str = "jdbc:postgresql://" + credentials["host"] + ":5432/postgres_db"
        
        properties = {       
            "user": credentials["user"],
            "password": credentials["password"],
            "driver": "org.postgresql.Driver",
            "fetchsize": "10000",
            "partitionColumn": partition_column,
            "upperBound": str(upper_bound),
            "lowerBound": str(lower_bound),
            "numPartitions": str(num_partitions)
        }

        data_df = self.spark.read.jdbc(
            url=connection_str,
            properties=properties,
            table=table
        )

        return data_df
    
    def _transform_data(self, df_land_use, df_regions):
        """Perform geospatial transformations and calculations"""
        try:    
            df_full = df_land_use.join(F.broadcast(df_regions), df_land_use['region_id'] == df_regions['id'], 'inner')   
            df_full = df_full.withColumn("geo_land_use_mod", F.expr("ST_Buffer(ST_MakeValid(ST_GeomFromWKT(land_geometry)), 0.0000001)"))\
                            .withColumn("geo_region_mod", F.expr("ST_MakeValid(ST_GeomFromWKT(region_geometry))"))
            
        except Exception as e:
            logging.error("Error caused in df_full with error: %s", e)
            raise
        
        try: 
            df_grouped = df_full.groupBy(
                "region_id", "season_id", "land_type_id", "geo_region_mod"
            ).agg(
                F.expr("ST_MakeValid(ST_Union_Aggr(geo_land_use_mod))").alias("geo_land_use_mod")
            )
        except Exception as e:
            logging.error("Error caused in df_grouped with error: %s", e)
            raise

        try:
            df_result = df_grouped.withColumn(
                "area",
                F.expr("""
                    ST_Area(
                        ST_Transform(
                            ST_Buffer(
                                ST_MakeValid(
                                    ST_Intersection(
                                        geo_land_use_mod,
                                        geo_region_mod
                                    )
                                ),
                                0
                            ),
                            'EPSG:3857',
                            'EPSG:5880'
                        )
                    )
                """)
            )
        except Exception as e:
            logging.error("Error caused in df_result with error: %s", e)
            raise

        return df_result.select("region_id", "season_id", "land_type_id", "area")
    
    def _save_to_s3(self, data_df, s3_path, num_partitions=10):
        """Save the processed data to S3 in CSV format"""
        try:
            data_df.repartition(num_partitions)\
                .write\
                .option("maxRecordsPerFile", 10000)\
                .option("header", "false")\
                .option("delimiter", ",")\
                .mode("overwrite")\
                .csv(s3_path)
            
            logging.info('Data saved in S3!')
        except Exception as e:
            logging.error('Error uploading Spark data to S3: %s', e)
            raise
    
    def process_geospatial_data(self, s3_output_path):
        """Main method to execute the complete geospatial processing pipeline"""
        # Query for land use area bounds
        query_bounds_land_use = "(SELECT max(property_id), min(property_id) FROM core.land_use_areas) AS bounds_land_use"
        max_land_use, min_land_use = self._read_data_bounds(query_bounds_land_use)
        query_land_use = "(SELECT region_id, season_id, land_type_id, property_id, ST_AsText(geometry) as land_geometry FROM core.land_use_areas) AS land_use_data"
        df_land_use = self._read_data_from_db(query_land_use, "property_id", max_land_use, min_land_use, 100)
        
        # Query for geographic region bounds
        query_bounds_regions = "(SELECT max(id), min(id) FROM core.geographic_regions) AS bounds_geographic_regions"
        max_regions, min_regions = self._read_data_bounds(query_bounds_regions)
        query_regions = "(SELECT id, ST_AsText(geometry) as region_geometry FROM core.geographic_regions) AS regions_data"
        df_regions = self._read_data_from_db(query_regions, "id", max_regions, min_regions, 100)

        # Data processing
        df_processed = self._transform_data(df_land_use, df_regions)
        self._save_to_s3(df_processed, s3_output_path)
        
        logging.info("Geospatial processing completed successfully!")

if __name__ == '__main__':
    env = sys.argv[1]
    s3_path = "s3a://s3_path/geospatial_analysis/"
    
    processor = GeospatialProcessor(env)
    processor.process_geospatial_data(s3_path)
