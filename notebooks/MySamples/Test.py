# Databricks notebook source
#added new line

# COMMAND ----------

#Creating widgets for leveraging parameters, and printing the parameters
dbutils.widgets.text("input", "","")
y = dbutils.widgets.get("input")
print ("Param -\'input':")
print (y)


# COMMAND ----------

# Creating widgets for leveraging parameters, and printing the parameters
#def myfunc():
#  dbutils.widgets.text("input", "","")
#  y = dbutils.widgets.get("input")
#  print ("Param -\'input':")
#  print (y)
#
#while True:
#  myfunc()

# COMMAND ----------

