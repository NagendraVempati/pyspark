{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "2448a8b3",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "import findspark\n",
    "findspark.init()\n",
    "import pyspark\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark import SparkContext,SparkConf\n",
    "import sys\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "41a1db18",
   "metadata": {},
   "outputs": [],
   "source": [
    "def spark_session():\n",
    "    spark = SparkSession.builder.master('local').appName('Mannara').getOrCreate()\n",
    "    return spark"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "ae0e0065",
   "metadata": {},
   "outputs": [],
   "source": [
    "spk = spark_session()\n",
    "sc =spk.sparkContext"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "6a080969",
   "metadata": {},
   "outputs": [],
   "source": [
    "data =[(\"James\",\"\",\"Smith\",\"36636\",\"M\",3000),\n",
    "    (\"Michael\",\"Rose\",\"\",\"40288\",\"M\",4000),\n",
    "    (\"Robert\",\"\",\"Williams\",\"42114\",\"M\",4000),\n",
    "    (\"Maria\",\"Anne\",\"Jones\",\"39192\",\"F\",4000),\n",
    "    (\"Jen\",\"Mary\",\"Brown\",\"\",\"F\",-1)\n",
    "  ]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "d02f5c04",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.types import StructType,StructField, StringType, IntegerType"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "3781ae2e",
   "metadata": {},
   "outputs": [],
   "source": [
    "columns = StructType([StructField('Name',StringType(),True),StructField('LastName',StringType(),True),\\\n",
    "    StructField('frndname',StringType(),True),StructField('pinNumber',StringType(),True),\\\n",
    "    StructField('Gender',StringType(),True),StructField('SomeNumber',IntegerType(),True)\\\n",
    "])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "id": "6c355773",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+--------+--------+---------+------+----------+\n",
      "|   Name|LastName|frndname|pinNumber|Gender|SomeNumber|\n",
      "+-------+--------+--------+---------+------+----------+\n",
      "|  James|        |   Smith|    36636|     M|      3000|\n",
      "|Michael|    Rose|        |    40288|     M|      4000|\n",
      "| Robert|        |Williams|    42114|     M|      4000|\n",
      "|  Maria|    Anne|   Jones|    39192|     F|      4000|\n",
      "|    Jen|    Mary|   Brown|         |     F|        -1|\n",
      "+-------+--------+--------+---------+------+----------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df = spk.createDataFrame(data = data, schema = columns)\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "163f610f",
   "metadata": {},
   "outputs": [],
   "source": []
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
