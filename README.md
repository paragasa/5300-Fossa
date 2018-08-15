# 5300-Fossa

this is the final milestone implementation, test scripts down here

SQL> test
SQL> create table foo (id int, data text)
SQL> insert into foo values (1,"one");insert into foo values(2,"two"); insert into foo values (2, "another two")
SQL> select * from foo
SQL> create index fxx on foo (id)
SQL> show index from foo
SQL> delete from foo where data = "two"
SQL> select * from foo
SQL> create index fxx on foo (id)
SQL> show index from foo
SQL> insert into foo values (4,"four")
SQL> select * from foo
SQL> quit