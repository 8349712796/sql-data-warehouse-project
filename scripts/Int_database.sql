
/*
Create Database & Schemas
Script purpose: 
This script create a new database named 'Datawarehouse' after checking if it already exist.
if the databse exists, it is dropped & recreated. Additionally script setup three schemas
within the database: 'Bronze', 'Silver', 'Gold'

Warning:
Running this script will drop your database permanently if it exists.
All the data in the database wil permanently will be deleted. Proceed with caution
and ensure you have proper backups before running this script.
*/


Use master;
Go

--Drop & recreate Datawarehouse
If exists (Select 1 From sys.databases where name = 'Datawarehouse')
Begin
	Alter Database Datawarehouse Set Single_User With Rollback Immediate;
	Drop Database Datawarehouse
End;
Go

--Create the database 'Datawarehouse'
Create Database Datawarehouse;
Go

Use Datawarehouse
Go

-- Create Schemas
Create Schema Bronze;
Go

Create Schema Silver;
Go

Create Schema Gold;
Go
