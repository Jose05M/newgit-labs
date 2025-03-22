from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("thyroid_cancer_risk")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_people="dataset.csv"
    df_people = spark.read.csv(path_people,header=True,inferSchema=True)
    #df_people = df_people.withColumnRenamed("date of birth", "birth")
    df_people.createOrReplaceTempView("people")
    query='DESCRIBE people'
    spark.sql(query).show(20)

    #query="""SELECT Gender, Country, Age FROM people WHERE Gender=="male" ORDER BY `Age`"""
    #df_people_names = spark.sql(query)
    #df_people_names.show(20)

    query='SELECT Gender, Country, Smoking, `Age` FROM people WHERE `Age` BETWEEN "35" AND "50" ORDER BY `Age`'
    df_people_35_50 = spark.sql(query)
    df_people_35_50.show(20)
    results = df_people_35_50.toJSON().collect()
    #print(results)
    df_people_35_50.write.mode("overwrite").json("results")
    #df_people_1903_1906.coalesce(1).write.json('results/data_merged.json')
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    #query='SELECT sex,COUNT(sex) FROM people WHERE birth BETWEEN "1903-01-01" AND "1911-12-31" GROUP BY sex'
    #df_people_1903_1906_sex = spark.sql(query)
    #df_people_1903_1906_sex.show()
    spark.stop()
