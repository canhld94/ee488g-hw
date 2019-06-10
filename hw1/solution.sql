-- DATABASE:
-- Customer = {customerID*, rstName, lastName, income, birthDate}
-- Account = {accNumber*, type, balance, branchNumber(FK-Branch)}
-- Owns = {customerID*(FK-Customer), accNumber*(FK-Account)}
-- Transactions = {transNumber*, accNumber*(FK-Account), amount}
-- Employee = {sin*, rstName, lastName, salary, branchNumber(FK-Branch)}
-- Branch = {branchNumber*, branchName, managerSIN(FK-Employee), budget}


-- First name, last name, income of customers whose income is within [60,000, 70,000],
-- order by income (desc), lastName, rstName
select firstName, lastName, income
from Customer
where income >= 60000 and income <= 70000
order by income desc, lastName, firstName
limit 5;

-- SIN, branch name, salary and manager's salary - salary (that is, the salary of the
-- employee's manager minus salary of the employee) of all employees in New York,
-- London or Berlin, order by ascending (manager salary - salary).
select sin, branchName, salary, (managerSalary - salary) as salaryGap
from 
    (select branchNumber, salary as managerSalary 
    from Employee natural join Branch 
    where managerSIN = sin) as S
    natural join Employee
    natural join Branch
where (branchName = 'New York' or branchName = 'London' or branchName = 'Berlin')
order by salaryGap
limit 5;

-- First name, last name, and income of customers whose income is at least twice the
-- income of any customer whose lastName is Butler, order by last name then rst
-- name.
select firstName, lastName, income
from Customer
where income > any 
	(select 2*income from Customer 
     where lastName = 'Butler')
order by lastName, firstName
limit 5;

-- Customer ID, income, account numbers and branch numbers of customers with
-- income greater than 90,000 who own an account at both London and Latveria
-- branches, order by customer ID then account number. The result should contain all
-- the account numbers of customers who meet the criteria, even if the account itself
-- is not held at London or Latveria.
select Customer.customerID, income, Owns.accNumber, branchNumber
from Customer, Owns, Account
where Customer.customerID = Owns.customerID 
and Owns.accNumber = Account.accNumber
and Customer.customerID in
    (select Customer.customerID
    from Customer, Owns, Account, Branch
    where income > 90000
    and Customer.customerID = Owns.customerID
    and Owns.accNumber = Account.accNumber
    and Account.branchNumber = Branch.branchNumber
    and (branchName = 'London' or branchName = 'Latveria')
    )
and Customer.customerID in
    (select Customer.customerID
    from Customer, Owns, Account, Branch
    where income > 90000
    and Customer.customerID = Owns.customerID
    and Owns.accNumber = Account.accNumber
    and Account.branchNumber = Branch.branchNumber
    and (branchName = 'Latveria')
    ) 
and Customer.customerID in
    (select Customer.customerID
    from Customer, Owns, Account, Branch
    where income > 90000
    and Customer.customerID = Owns.customerID
    and Owns.accNumber = Account.accNumber
    and Account.branchNumber = Branch.branchNumber
    and (branchName = 'London')
    ) 
order by customerID, accNumber
limit 5;

-- Customer ID, types, account numbers and balances of business (type BUS) and
-- savings (type SAV) accounts owned by customers who own at least one business
-- account or at least one savings account, order by customer ID, then type, then
-- account number.
select Customer.customerID, type, Account.accNumber, balance
from Customer, Owns, Account
where Customer.customerID = Owns.customerID
and Owns.accNumber = Account.accNumber
and (type = 'BUS' or type = 'SAV')
order by customerID, type, accNumber
limit 5;

-- Branch name, account number and balance of accounts with balances greater than
-- $110,000 held at the branch managed by Phillip Edwards, order by account number.
select branchName, accNumber, balance
from Account, Branch, Employee
where balance > 110000
and Account.branchNumber = Branch.branchNumber
and Branch.managerSIN = Employee.sin
and firstName = 'Phillip'
and lastName = 'Edwards'
order by accNumber
limit 5;

-- Customer ID of customers who have an account at the New York branch, who do
-- not own an account at the London branch and who do not co-own an account with
-- another customer who owns an account at the London branch, order by customer
-- ID. The result should not contain duplicate customer IDs.
select distinct Owns.customerID 
from Owns, Account, Branch
where Owns.accNumber = Account.accNumber
and Account.branchNumber = Branch.branchNumber
and Branch.branchName =  'New York'
and Owns.customerID not in 
	(select Owns.customerID 
    from Owns, Account, Branch
    where Owns.accNumber = Account.accNumber
    and Account.branchNumber = Branch.branchNumber
    and Branch.branchName =  'London')
and Owns.customerID not in
    (select S1.customerID
    from Owns S1, Owns S2, Account, Branch
    where S1.accNumber = S2.accNumber
    and S1.customerID <> S2.customerID
    and S1.accNumber = Account.accNumber
    and Account.branchNumber = Branch.branchNumber
    and Branch.branchName =  'London')
order by customerID
limit 5;

-- SIN, rst name, last name, and salary of employees who earn more than $70,000, if
-- they are managers show the branch name of their branch in a fth column (which
-- should be NULL/NONE for most employees), order by branch name (desc). You
-- must use an outer join in your solution (which is the easiest way to do it).
select S.sin, S.firstName, S.lastName, S.salary, T.branchName
from (
        (select sin, firstName, lastName, salary
        from Employee
        where salary > 70000) as S
     left outer join
        (select sin, firstName, lastName, salary, branchName
        from Employee, Branch
        where Employee.sin = Branch.managerSIN
        and salary > 70000) as T 
     on S.sin = T.sin
     )
order by branchName desc
limit 5;

-- Exactly as question eight, except that your query cannot include any join operation.
select sin, firstName, lastName, salary,
case
when sin in 
    (select sin
    from Employee, Branch
    where Employee.sin = Branch.managerSIN)
then 
    (select branchName
    from Branch
    where managerSIN = sin)
else NULL
end as branchName
from Employee
where salary > 70000
order by branchName desc
limit 5;

-- Customer ID, rst name, last name and income of customers who have income
-- greater than 5000 and own accounts in all of the branches that Helen Morgan owns
-- accounts in, order by income in decreasing order.
select customerID, firstName, lastName, income
from
    (select Customer.customerID, firstName, lastName, income, count(distinct branchNumber) as totBranch
    from Customer , Owns, Account
    where Owns.customerID = Customer.customerID
    and Account.accNumber = Owns.accNumber 
    and branchNumber in 
        (select branchNumber
        from Customer, Owns, Account
        where Account.accNumber = Owns.accNumber 
        and Owns.customerID = Customer.customerID
        and firstName =  'Helen'
        and lastName = 'Morgan')
        group by customerID, firstName, lastName, income) as S
        where income > 5000
        and totBranch >=
            (select count(distinct branchNumber)
            from Customer, Owns, Account
            where Account.accNumber = Owns.accNumber 
            and Owns.customerID = Customer.customerID
            and firstName =  'Helen'
            and lastName = 'Morgan')
order by income desc
limit 5;

-- SIN, rst name, last name and salary of the lowest paid employee (or employees) of
-- the London branch, order by sin.
select sin, firstName, lastName, salary
from Employee, Branch
where branchName = 'London'
and Branch.branchNumber = Employee.branchNumber
and salary <= all 
	(select salary
    from Employee, Branch
    where branchName = 'London'
    and Branch.branchNumber = Employee.branchNumber)
order by sin
limit 5;

-- Branch name, and the dierence of maximum and minimum (salary gap) and aver-
-- age salary of the employees at each branch, order by branch name.
select branchName, (maxSalary-minSalary) as salaryGap, avgSalary
from Branch,
(select branchNumber, min(salary) as minSalary, max(salary) as maxSalary, avg(salary) as avgSalary
from Employee
group by branchNumber) as S
where Branch.branchNumber = S.branchNumber
order by branchName
limit 5;

-- Count of the number of employees working at the New York branch and Count of
-- the number of dierent last names of employees working at the New York branch
-- (two numbers in a single row).
select count(sin), count(distinct lastName)
from
    (select sin, lastName, branchName
    from Employee, Branch
    where Employee.branchNumber = Branch.branchNumber
    and branchName = 'New York') as S
limit 5;

-- Sum of the employee salaries (a single number) at the New York branch.
select sum(salary)
from
(select sin, branchName, salary
from Employee, Branch
where Employee.branchNumber = Branch.branchNumber
and branchName = 'New York') as S
limit 5;

-- Customer ID, rst name and last name of customers who own accounts from four
-- dierent branches, order by Last Name and rst Name.
select customerID, firstName, lastName
from
(select Customer.customerID, firstName, lastName, income, count(distinct branchNumber) as totBranch
from Customer , Owns, Account
where Owns.customerID = Customer.customerID
and Account.accNumber = Owns.accNumber 
group by customerID, firstName, lastName, income) as S
where totBranch >= 4
order by lastName, firstName
limit 5;

-- Average income of customers older than 60 and average income of customers younger
-- than 20, the result must have two named columns, with one row, in one result set
select * from
    (select avg(income) as 'overSixtyAvgSalary'
     from
        (select income, 
         case 
         when timestampdiff(year, birthData, now()) > 60 then 'overSixty'
         else NULL
         end as age
         from Customer) as S
         where age =  'overSixty'
         group by age) as P
    join
    (select avg(income) as 'underTwentyAvgSalary'
    from
        (select income, 
        case 
        when timestampdiff(year, birthData, now()) < 20 then 'underTwenty'
        else NULL
        end as age
        from Customer) as S
        where age =  'underTwenty'
        group by age) as Q

-- Customer ID, last name, rst name, income, and average account balance of cus-
-- tomers who have at least three accounts, and whose last names begin with S and
-- contain an e (e.g. Steve), order by customer ID.
select customerID, lastName, firstName, income, avgBalance
from Customer natural join
    (select customerID, avg(balance) as avgBalance, count(accNumber) as totAccount
    from Owns natural join Account
    group by customerID
    having totAccount > 3) as S
where lastName like 'S%e%'
order by customerID
limit 5;

-- Account number, balance, sum of transaction amounts, and balance - transaction
-- sum for accounts in the London branch that have at least 10 transactions, order by
-- transaction sum.
select accNumber, balance, totAmount, (balance-totAmount) as curBalance
from Account natural join
    (select accNumber, count(transNumber) as totTrans, sum(amount) as totAmount
    from Transactions
    group by accNumber
    having totTrans >= 10) as S
    natural join Branch
where branchName =  'London'
order by totAmount
limit 5;

-- Branch name, account type, and average transaction amount of each account type
-- for each branch for branches that have at least 50 accounts of any type, order by
-- branch name, then account type.
select branchName, type, avg(amount) as avgAmount
from
    (select branchNumber, branchName
     from 
        (select branchNumber, count(accNumber) as totAccount
         from Account
         group by branchNumber
         having totAccount >= 50) as S
         natural join Branch) as Q
     natural join
        (select type, amount, branchNumber
        from Account natural join Transactions) as P
group by branchName, type
order by branchName, type
limit 5;

-- Account type, account number, transaction number and amount of transactions of
-- accounts where the average transaction amount is greater than three times the (over-
-- all) average transaction amount of accounts of that type. For example, if the average
-- transaction amount of all business accounts is $2,000 then return transactions from
-- business accounts where the average transaction amount for that account is greater
-- than $6,000. Order by account type, then account number and nally transaction
-- number. Note that all transactions of qualifying accounts should be returned even
-- if they are less than the average amount of the account type.
select type, Transactions.accNumber, transNumber, amount
from Transactions, Account
where Transactions.accNumber = Account.accNumber
and Transactions.accNumber in
    (select accNumber
     from
        (select type, accNumber, avg(amount) as accAvgAmount
         from 
            (select * from Account natural join Transactions) as T
         group by accNumber) as S
     natural join
        (select type, avg(amount) as typeAvgAmount
         from Account natural join Transactions
         group by type) as Q
     where accAvgAmount >= 3* typeAvgAmount)
order by type, accNumber, transNumber
limit 5;

