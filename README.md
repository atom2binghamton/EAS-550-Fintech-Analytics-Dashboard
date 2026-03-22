# EAS-550-Fintech-Analytics-Dashboard
GENERAL SCRIPT TESTING INSTRUCTIONS:
1. git checkout -b <name of test branch> (this will also switch to the branch after creating it)
2. Make a minimal change to the code base such as a comment and save it (since the yml runs the scripts on a pull request)
3. git add .
4. git commit -m "Testing scripts"
5. git push origin <name of test branch>
6. Open the pull request to initiate the script execution (repository > pull requests > new pull request > select your test branch)
7. Let the GitHub actions checks run
8. Check your neon database and run some test queries and security checks by trying to perform an action that is not authorized
9. For deletion, navigate to the test branch in pull requests and click close pull request to initiate the neon branch deletion
10. Link to youtube video demonstration: <put it here>

HOW TO TEST SECURITY SCRIPT IN NEON SQL EDITOR:
(Run these commands one at a time sequentially)
1. GRANT "Analyst" TO CURRENT_USER;
2. GRANT CONNECT ON DATABASE neondb TO "Analyst";
3. SELECT current_user; -- should be owner here
4. SET ROLE "Analyst";
5. SELECT current_user; -- should be analyst now
6. SELECT * FROM product_categories LIMIT 5;
7. INSERT INTO product_categories (ProductCategoryID, ProductCategoryName) VALUES (999, 'Test Category');
8. DELETE FROM product_categories WHERE ProductCategoryID = 1;
9. RESET ROLE;
10. SELECT current_user; -- should be back to owner

You should be able to see that you've switched to the Analyst role after setting the role. Select should succeed but insert and delete should have permissions denied.