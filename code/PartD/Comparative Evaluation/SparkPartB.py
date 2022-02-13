import pyspark

sc = pyspark.SparkContext()

def is_good_line_trans(line):
    try:
        fields = line.split(",")
        if len(fields) != 7:
            return False
        val = int(fields[3])
        if val <= 0:
            return False
        return True
    except:
        return False

def is_good_line_Con(line):
    try:
        fields = line.split(",")
        if len(fields) != 5:
            return False

        return True
    except:
        return False



lines_trans = sc.textFile("/data/ethereum/transactions")
clean_lines = lines_trans.filter(is_good_line_trans)
trans_values = clean_lines.map(lambda l: (l.split(",")[2], int(l.split(",")[3])))

lines_con = sc.textFile("/data/ethereum/contracts")
clean_lines_con = lines_con.filter(is_good_line_Con)
con_adds = clean_lines_con.map(lambda k: (k.split(",")[0], 0))

con_trans = trans_values.join(con_adds)
cleaned_values = con_trans.map(lambda w: (w[0],w[1][0]))
total_values = cleaned_values.reduceByKey(lambda a,b: a+b)

top10 = total_values.takeOrdered(10, key = lambda x: -x[1])
for i in top10:
    print(i)


