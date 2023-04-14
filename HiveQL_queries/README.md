
Data: Dualcore has a
sample of the data (loyalty_data.txt) that contains information about customers who have signed
up for the program, including their customer ID, first name, last name, email, loyalty level,
phone numbers , a list of past order IDs, and a struct that summarizes the minimum, maximum,
average, and total value of past orders.

Problem 1 Queries: 
-  ‘hw5_1.hql’: creates the table loyalty_program. 
-  ‘hw5_2.hql’ loads the data in loyalty_data.txt into Hive. 
-  ‘hw5_3.hql’ selects the HOME phone number for customer ID 1200866.
-  ‘hw5_4.hql’ selects the third element from the order_ids for customer ID
1200866.

Problem 2 Queries: 
- ‘hw5_5.hql’ finds how many products have been bought by the
customer 1071189
- ‘hw5_6.hql’ finds how many customers have spent more than
300000 on the total price of all products that s/he has bought.
- ‘hw5_7.hql’ lists the customers (cust_id only) who have not placed
any order

Problem 3 Queries: 
- ‘hw5_8.hql’ finds the product with the lowest average rating among
products with at least 50 ratings.

- ‘hw5_9.hql’ finds the five most common trigrams (three-word
combinations) in the comments for the product that you have identified in the previous
question

- Among the patterns you see in the result of the previous question is the phrase “ten times
more.” This might be related to the complaints that the product is too expensive.  ‘hw5_10.hql’ lists the product’s comments that contain the phrase.
