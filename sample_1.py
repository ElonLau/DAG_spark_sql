


# Add col running_total that sums diff_min col in each group
query = """
SELECT train_id, station, time, diff_min,
SUM(diff_min) OVER (PARTITION BY train_id ORDER BY time) AS running_total
FROM schedule
"""

# sum(diff_min) is alias as running_total and the output is
# ordered in ascending order by time and partitioned by train_id

# Run the query and display the result
spark.sql(query).show()

"""

<script.py> output:
    +--------+-------------+-----+--------+-------------+
    |train_id|      station| time|diff_min|running_total|
    +--------+-------------+-----+--------+-------------+
    |     217|       Gilroy|6:06a|     9.0|          9.0|
    |     217|   San Martin|6:15a|     6.0|         15.0|
    |     217|  Morgan Hill|6:21a|    15.0|         30.0|
    |     217| Blossom Hill|6:36a|     6.0|         36.0|
    |     217|      Capitol|6:42a|     8.0|         44.0|
    |     217|       Tamien|6:50a|     9.0|         53.0|
    |     217|     San Jose|6:59a|    null|         53.0|
    |     324|San Francisco|7:59a|     4.0|          4.0|
    |     324|  22nd Street|8:03a|    13.0|         17.0|
    |     324|     Millbrae|8:16a|     8.0|         25.0|
    |     324|    Hillsdale|8:24a|     7.0|         32.0|
    |     324| Redwood City|8:31a|     6.0|         38.0|
    |     324|    Palo Alto|8:37a|    28.0|         66.0|
    |     324|     San Jose|9:05a|    null|         66.0|
    +--------+-------------+-----+--------+-------------+

"""


# -----------------------------------------------------------------------------

# Give the identical result in each command
spark.sql('SELECT train_id, MIN(time) AS start FROM schedule GROUP BY train_id').show()
df.groupBy('train_id').agg({'time':'min'}).withColumnRenamed('min(time)', 'start').show()

# Print the second column of the result
spark.sql('SELECT train_id, MIN(time), MAX(time) FROM schedule GROUP BY train_id').show()
result = df.groupBy('train_id').agg({'time':'min', 'time':'max'})
result.show()
print(result.columns[1])


"""
<script.py> output:
    +--------+-----+
    |train_id|start|
    +--------+-----+
    |     217|6:06a|
    |     324|7:59a|
    +--------+-----+

    +--------+-----+
    |train_id|start|
    +--------+-----+
    |     217|6:06a|
    |     324|7:59a|
    +--------+-----+

    +--------+---------+---------+
    |train_id|min(time)|max(time)|
    +--------+---------+---------+
    |     217|    6:06a|    6:59a|
    |     324|    7:59a|    9:05a|
    +--------+---------+---------+

    +--------+---------+
    |train_id|max(time)|
    +--------+---------+
    |     217|    6:59a|
    |     324|    9:05a|
    +--------+---------+

    max(time)


"""

# --------------------------------------------------------------------------

# Convert window function from dot notation to SQL

window = Window.partitionBy('train_id').orderBy('time')
dot_df = df.withColumn('diff_min',
                    (unix_timestamp(lead('time', 1).over(window),'H:m')
                     - unix_timestamp('time', 'H:m'))/60)

# Create a SQL query to obtain an identical result to dot_df
query = """
SELECT *,
(UNIX_TIMESTAMP(LEAD(time, 1) OVER (PARTITION BY train_id ORDER BY time),'H:m')
 - UNIX_TIMESTAMP(time, 'H:m'))/60 AS diff_min
FROM schedule
"""
sql_df = spark.sql(query)
sql_df.show()


"""

<script.py> output:
    +--------+-------------+-----+--------+
    |train_id|      station| time|diff_min|
    +--------+-------------+-----+--------+
    |     217|       Gilroy|6:06a|     9.0|
    |     217|   San Martin|6:15a|     6.0|
    |     217|  Morgan Hill|6:21a|    15.0|
    |     217| Blossom Hill|6:36a|     6.0|
    |     217|      Capitol|6:42a|     8.0|
    |     217|       Tamien|6:50a|     9.0|
    |     217|     San Jose|6:59a|    null|
    |     324|San Francisco|7:59a|     4.0|
    |     324|  22nd Street|8:03a|    13.0|
    |     324|     Millbrae|8:16a|     8.0|
    |     324|    Hillsdale|8:24a|     7.0|
    |     324| Redwood City|8:31a|     6.0|
    |     324|    Palo Alto|8:37a|    28.0|
    |     324|     San Jose|9:05a|    null|
    +--------+-------------+-----+--------+


"""


# --------------------------------------------------------------------------


# Load the dataframe
df = spark.read.load('sherlock_sentences.parquet')

# Filter and show the first 5 rows
df.where('id > 70').show(5, truncate=False)

"""
<script.py> output:
    +--------------------------------------------------------+---+
    |clause                                                  |id |
    +--------------------------------------------------------+---+
    |i answered                                              |71 |
    |indeed i should have thought a little more              |72 |
    |just a trifle more i fancy watson                       |73 |
    |and in practice again i observe                         |74 |
    |you did not tell me that you intended to go into harness|75 |
    +--------------------------------------------------------+---+
    only showing top 5 rows
"""

# --------------------------------------------------------------------------


# Split the clause column into a column called words
split_df = clauses_df.select(split('clause', ' ').alias('words'))
split_df.show(5, truncate=False)

# Explode the words column into a column called word
exploded_df = split_df.select(explode('words').alias('word'))
exploded_df.show(10)

# Count the resulting number of rows in exploded_df
print("\nNumber of rows: ", exploded_df.count())



"""

<script.py> output:

    +-----------------------------------------------+
    |words                                          |
    +-----------------------------------------------+
    |[title]                                        |
    |[the, adventures, of, sherlock, holmes, author]|
    |[sir, arthur, conan, doyle, release, date]     |
    |[march, 1999]                                  |
    |[ebook, 1661]                                  |
    +-----------------------------------------------+
    only showing top 5 rows

    +----------+
    |      word|
    +----------+
    |     title|
    |       the|
    |adventures|
    |        of|
    |  sherlock|
    |    holmes|
    |    author|
    |       sir|
    |    arthur|
    |     conan|
    +----------+
    only showing top 10 rows


    Number of rows:  1279

"""

# --------------------------------------------------------------------------

# Find the top 10 sequences of five words
query = """
SELECT w1, w2, w3, w4, w5, COUNT(*) AS count FROM (
   SELECT word AS w1,
   LEAD(word,1) OVER(partition by part order by id) AS w2,
   LEAD(word,2) OVER(partition by part order by id) AS w3,
   LEAD(word,3) OVER(partition by part order by id) AS w4,
   LEAD(word,4) OVER(partition by part order by id) AS w5
   FROM text
)
GROUP BY w1, w2, w3, w4, w5
ORDER BY count DESC
LIMIT 10 """
df = spark.sql(query)
df.show()

"""

<script.py> output:
    +-----+---------+------+-------+------+-----+
    |   w1|       w2|    w3|     w4|    w5|count|
    +-----+---------+------+-------+------+-----+
    |   in|      the|  case|     of|   the|    4|
    |    i|     have|    no|  doubt|  that|    3|
    | what|       do|   you|   make|    of|    3|
    |  the|   church|    of|     st|monica|    3|
    |  the|      man|   who|entered|   was|    3|
    |dying|reference|    to|      a|   rat|    3|
    |    i|       am|afraid|   that|     i|    3|
    |    i|    think|  that|     it|    is|    3|
    |   in|      his| chair|   with|   his|    3|
    |    i|     rang|   the|   bell|   and|    3|
    +-----+---------+------+-------+------+-----+

"""

# --------------------------------------------------------------------------
