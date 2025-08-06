# mock-data-governance
working with messy data and using data quality practices to clean it up and practice SQL/Python automation


7/23 - database created inside of DB Browser for SQLite
  created an update query and fixed null account numbers
  created two queries inside of DB Browser and exported them
  created 'checking for null emails' and 'phone number null list' through DB Browser

7/25 - cleaned up the code and removed the massive query for loop
  converted into a single function that can take a query and perform the desired effect

7/26 - added a way to NOT output good queries - ie queries with no issues
  those are now stored in a list and printed out into a textfile

7/27 - added a new folder called 'output' to store all of the output from the script to help clean up the directory

7/28 - DATE('now') was not being used properly; the database stores dates as mm/dd/yyyy in String format
  had to figure out how to convert it properly
  utilized AI to find a resolution

8/1 - simple cleanup of script; fixed some of the comments to make them easier to remember what and why it was done


