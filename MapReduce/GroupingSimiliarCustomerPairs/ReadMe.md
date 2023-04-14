We'll use publicly available Amazon customer review datasets(https://s3.amazonaws.com/amazon-reviews) to do an
analysis. This time we are interested in pairs of users with similar preferences. Specifically, we want to
know the pairs of users (“customer_id” in the review data) who gave the rating (“star_rating” in the
review data) of at least 4 to at least 3 common products. The output must include pairs of users
(“customer_id”) and the list of common products (“product_id” in the review data). The output must be
sorted by the first customer_id in the pairs of users and must not contain duplicates. Also, a user pair does
not have order. That is to say, for any two customer_id u1 and u2, <u1, u2> and <u2, u1> are considered
as the same pair.

Utilizes a ustom WritableComparable class to hold the user pair and two mapreduce jobs. 
The first job creates the pairs of users who gave a rating of at least 4 to a common product.
o The 2nd job aggregates all products of the same pair of users and outputs the pair of users if
there are at least 3 common products. We chain the two jobs together. 

UserPairWritable.java was provided by instructor for this assignment. 
