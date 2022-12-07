# Databricks notebook source
df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/Masking.csv")
display(df1)

# COMMAND ----------

def mask_email_func(coval):
    mail_user = coval.split("@")[0]
    n = len (mail_user)
    charlist = list(mail_user)
    charlist[1:int(n-1)] = '*'*int(n-2)
    out = "".join(charlist)+"@"+coval.split("@")[1]
    return out

# COMMAND ----------

def mask_mobile_func(colval):
    n = len(colval)
    charlist = list(colval)
    charlist[2:int(n-1)] = '*'*int(n-4)
    return "".join(charlist)

# COMMAND ----------


