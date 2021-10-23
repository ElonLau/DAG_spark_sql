# Caching, Logging, and the Spark UI

# Returns true if the value is a nonempty vector
nonempty_udf = udf(lambda x:
    True if (x and hasattr(x, "toArray") and x.numNonzeros())
    else False, BooleanType())

# Returns first element of the array as string
s_udf = udf(lambda x: str(x[0]) if (x and type(x) is list and len(x) > 0)
    else '', StringType())

# -----------------------------------------------------------------

# Show the rows where doc contains the item '5'
df_before.where(array_contains('doc', '5')).show()

# UDF removes items in TRIVIAL_TOKENS from array
rm_trivial_udf = udf(lambda x:
                     list(set(x) - TRIVIAL_TOKENS) if x
                     else x,
                     ArrayType(StringType()))

# Remove trivial tokens from 'in' and 'out' columns of df2
df_after = df_before.withColumn('in', rm_trivial_udf('in'))\
                    .withColumn('out', rm_trivial_udf('out'))

# Show the rows of df_after where doc contains the item '5'
df_after.where(array_contains('doc','5')).show()


"""

<script.py> output:
    +--------------------+--------------------+--------------+
    |                 doc|                  in|           out|
    +--------------------+--------------------+--------------+
    |[she, left, this,...|[she, left, this,...|           [5]|
    |[he, had, no, occ...|[he, had, no, occ...|           [5]|
    |           [5, vols]|                 [5]|        [vols]|
    |[on, the, night, ...|[on, the, night, ...|        [town]|
    |[by, hurried, and...|[by, hurried, and...|[philadelphia]|
    +--------------------+--------------------+--------------+

    +--------------------+--------------------+--------------+
    |                 doc|                  in|           out|
    +--------------------+--------------------+--------------+
    |[she, left, this,...|[by, this, with, ...|            []|
    |[he, had, no, occ...|[by, was, compani...|            []|
    |           [5, vols]|                  []|        [vols]|
    |[on, the, night, ...|[stationed, crowd...|        [town]|
    |[by, hurried, and...|[carpenter, by, c...|[philadelphia]|
    +--------------------+--------------------+--------------+

"""

# -----------------------------------------------------------------------------

# Selects the first element of a vector column
first_udf = udf(lambda x:
            float(x.indices[0])
            if (x and hasattr(x, "toArray") and x.numNonzeros())
            else 0.0,
            FloatType())

# Apply first_udf to the output column
df.select(first_udf("output").alias("result")).show(5)

"""
+------------------+
|            output|
+------------------+
|(12847,[65],[1.0])|
| (12847,[8],[1.0])|
|(12847,[47],[1.0])|
|(12847,[89],[1.0])|
|(12847,[94],[1.0])|
+------------------+
only showing top 5 rows
"""

# -----------------------------------------------------------------------------

# Score the model on test data
testSummary = df_fitted.evaluate(df_testset)

# Print the AUC metric
print("\ntest AUC: %.3f" % testSummary.areaUnderROC)

"""
<script.py> output:

    test AUC: 0.890

"""

# -----------------------------------------------------------------------------

# Apply the model to the test data
predictions = df_fitted.transform(df_testset).select(fields)

# Print incorrect if prediction does not match label
for x in predictions.take(8):
    print()
    if x.label != int(x.prediction):
        print("INCORRECT ==> ")
    for y in fields:
        print(y,":", x[y])


"""


<script.py> output:

    prediction : 1.0
    label : 1
    endword : him
    doc : ['and', 'pierre', 'felt', 'that', 'their', 'opinion', 'placed', 'responsibilities', 'upon', 'him']
    probability : [0.28537355252312796,0.714626447476872]

    prediction : 1.0
    label : 1
    endword : him
    doc : ['at', 'the', 'time', 'of', 'that', 'meeting', 'it', 'had', 'not', 'produced', 'an', 'effect', 'upon', 'him']
    probability : [0.4223142404987621,0.5776857595012379]

    INCORRECT ==>
    prediction : 0.0
    label : 1
    endword : him
    doc : ['bind', 'him', 'bind', 'him']
    probability : [0.5623411025382637,0.43765889746173625]

    prediction : 1.0
    label : 1
    endword : him
    doc : ['bolkonski', 'made', 'room', 'for', 'him', 'on', 'the', 'bench', 'and', 'the', 'lieutenant', 'colonel', 'sat', 'down', 'beside', 'him']
    probability : [0.3683499060175795,0.6316500939824204]


"""
