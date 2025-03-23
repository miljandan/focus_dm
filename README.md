#Focus DM

Focus DM was developed as a pilot project for giving support to ad hoc reporting and analysis tasks. 
It enhances the business value derived from cloud technology. The goal of this project is to cover 
operations in order to provide organizations with a better understanding of their cloud spending. 
It also helps them make informed decisions on how to allocate and manage their cloud costs. 

#Architecture 

Architecture was based on well defined routines which can be clearly split in operations for data staging, 
data transformation, data orchestration and data analysis tasks as well. It has also tendency to provide 
high code modularity and readability. It should be easy to extend this project with new analysis case scenarios.
Technology stack

All code was developed on Databricks Community Edition platform using python and its pyspark library. 
Data analysis was done with SQL code.

#Instructions

1. Import project folder into Databricks workspace
2. First create two new schemas (stg, dm) in Databricks catalog 
   Scripts are here: /focus_dm/common_utilities/database_scripts	
3. Run solution from /focus_dm/orchestrating_data/populate_focus_dm.py notebook.

#Additional Info

Project following use cases defined on: https://focus.finops.org/use-cases/
Mappings between source files and Focus standard was found here:
https://learn.microsoft.com/en-us/cloud-computing/finops/focus/convert
