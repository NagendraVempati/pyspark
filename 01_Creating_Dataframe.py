{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "a012f3a2",
   "metadata": {},
   "outputs": [],
   "source": [
    "import findspark\n",
    "findspark.init()\n",
    "import pyspark\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark import SparkContext,SparkConf\n",
    "import sys"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "1a785b95",
   "metadata": {},
   "outputs": [],
   "source": [
    "#conf = SparkConf().setMaster(\"local\").setAppName(\"Mannara\")\n",
    "#sc = SparkContext(conf=conf)\n",
    "spark = SparkSession.builder.master(\"local\").appName(\"Mannara\").getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "0cc4ada0",
   "metadata": {},
   "outputs": [],
   "source": [
    "data = ['id','name','location']\n",
    "values = [(1,'Nani','Hyderabad'),(2,'gani','Chennai'),(3,'pani','Banglore')]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "255dce97",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[(1, 'Nani', 'Hyderabad'), (2, 'gani', 'Chennai'), (3, 'pani', 'Banglore')]"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "rdd1 = spark.sparkContext.parallelize(values)\n",
    "rdd1.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "b2051e7b",
   "metadata": {},
   "outputs": [
    {
     "ename": "TypeError",
     "evalue": "_monkey_patch_RDD.<locals>.toDF() got an unexpected keyword argument 'data'",
     "output_type": "error",
     "traceback": [
      "\u001b[1;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[1;31mTypeError\u001b[0m                                 Traceback (most recent call last)",
      "Cell \u001b[1;32mIn[6], line 1\u001b[0m\n\u001b[1;32m----> 1\u001b[0m df1 \u001b[38;5;241m=\u001b[39m \u001b[43mrdd1\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43mtoDF\u001b[49m\u001b[43m(\u001b[49m\u001b[43mdata\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43m \u001b[49m\u001b[43mvalues\u001b[49m\u001b[43m,\u001b[49m\u001b[43mschema\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43m \u001b[49m\u001b[43mdata\u001b[49m\u001b[43m)\u001b[49m\n\u001b[0;32m      2\u001b[0m df1\u001b[38;5;241m.\u001b[39mprintSchema()\n",
      "\u001b[1;31mTypeError\u001b[0m: _monkey_patch_RDD.<locals>.toDF() got an unexpected keyword argument 'data'"
     ]
    }
   ],
   "source": [
    "df1 = rdd1.toDF()\n",
    "df1.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "33d3c4e1",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---+----+---------+\n",
      "| _1|  _2|       _3|\n",
      "+---+----+---------+\n",
      "|  1|Nani|Hyderabad|\n",
      "|  2|gani|  Chennai|\n",
      "|  3|pani| Banglore|\n",
      "+---+----+---------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df1.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "ffb14d9d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- _1: string (nullable = true)\n",
      " |-- _2: string (nullable = true)\n",
      " |-- _3: string (nullable = true)\n",
      "\n",
      "+---+----+--------+\n",
      "| _1|  _2|      _3|\n",
      "+---+----+--------+\n",
      "| id|name|location|\n",
      "+---+----+--------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "rdd2 = spark.sparkContext.parallelize([data])\n",
    "df2 = rdd2.toDF()\n",
    "df2.printSchema()\n",
    "df2.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "1d0c4511",
   "metadata": {},
   "outputs": [],
   "source": [
    "#here i am creating the dataframe without rdd using createdataFrame()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "999c977c",
   "metadata": {},
   "outputs": [],
   "source": [
    "columns = ['id','Name','Location']\n",
    "data = [(1,'Nani','Hyderabad'),(2,'Gani','Chennai'),(3,'pani','Banglore')]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "9a567bf4",
   "metadata": {},
   "outputs": [],
   "source": [
    "df = spark.createDataFrame(data, schema=columns)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "059682bd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---+----+---------+\n",
      "| id|Name| Location|\n",
      "+---+----+---------+\n",
      "|  1|Nani|Hyderabad|\n",
      "|  2|Gani|  Chennai|\n",
      "+---+----+---------+\n",
      "only showing top 2 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.show(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "5ac128ed",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---+----+---------+\n",
      "| id|Name| Location|\n",
      "+---+----+---------+\n",
      "|  1|Nani|Hyderabad|\n",
      "|  2|Gani|  Chennai|\n",
      "|  3|pani| Banglore|\n",
      "+---+----+---------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.show()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
