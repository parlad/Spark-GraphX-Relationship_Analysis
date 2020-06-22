from pyspark.sql.functions import *

df = spark.createDataFrame(data, ["features"])
df_as1 = df.alias("df_as1")
df_as2 = df.alias("df_as2")
joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
joined_df.select("df_as1.name", "df_as2.name", "df_as2.age").collect()