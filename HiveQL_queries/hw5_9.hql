USE dualcore;

--We discovered the worst rated product to have prod-id 1274673 from #8

SELECT explode(ngrams(sentences(lower(ratings.message)),3,5))
FROM ratings WHERE prod_id = 1274673;
