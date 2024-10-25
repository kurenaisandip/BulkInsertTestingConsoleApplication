﻿using System.Data.SqlClient;

namespace BulkInsertTestingConsoleApplication
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;
    
     class Program
 {
     private const string createDatabase = @"
CREATE DATABASE [MostWanted]

USE [MostWanted]
CREATE TABLE [dbo].[Person](
    [PersonId] [int] IDENTITY(1,1) NOT NULL,
    [Name] [nvarchar](100) NOT NULL,
    [DateOfBirth] [datetime] NULL,
    CONSTRAINT [PK_Person] PRIMARY KEY CLUSTERED ([PersonId] ASC))
";
     static void Main(string[] args)
     {
     
         var people = CreateSamplePeople(10000);
         
         using (var connection = new SqlConnection("Server=DESKTOP-LOHF909;Database=MostWanted;Trusted_Connection=True;"))
         {
             connection.Open();

             var stopwatch = new Stopwatch();

             // ------ SQL Bulk Insert
             // Warm up...
           // RecreateDatabase(connection);
             InsertDataUsingSqlBulkCopy(people, connection);

             // Measure
             stopwatch.Start();
             InsertDataUsingSqlBulkCopy(people, connection);
             Console.WriteLine("Bulk copy: {0}ms", stopwatch.ElapsedMilliseconds);

             // ------ Insert statements
             // Warm up...
            // RecreateDatabase(connection);
             InsertDataUsingInsertStatements(people, connection);

             // Measure
             stopwatch.Reset();
             stopwatch.Start();
             InsertDataUsingInsertStatements(people, connection);
             Console.WriteLine("Individual insert statements: {0}ms", stopwatch.ElapsedMilliseconds);
         }
     }

     private static void InsertDataUsingInsertStatements(IEnumerable<Person> people, SqlConnection connection)
     {
         using (var command = connection.CreateCommand())
         {
             command.CommandText = "INSERT INTO Person (Name, DateOfBirth) VALUES (@Name, @DateOfBirth)";
             var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar);
             var dobParam = command.Parameters.Add("@DateOfBirth", SqlDbType.DateTime);
             foreach (var person in people)
             {
                 nameParam.Value = person.Name;
                 dobParam.Value = person.DateOfBirth;
                 command.ExecuteNonQuery();
             }
         }
     }

     private static void InsertDataUsingSqlBulkCopy(IEnumerable<Person> people, SqlConnection connection)
     {
         var bulkCopy = new SqlBulkCopy(connection);
         bulkCopy.DestinationTableName = "Person";
         bulkCopy.ColumnMappings.Add("Name", "Name");
         bulkCopy.ColumnMappings.Add("DateOfBirth", "DateOfBirth");

         using (var dataReader = new ObjectDataReader<Person>(people))
         {
             bulkCopy.WriteToServer(dataReader);
         }
     }

     private static void RecreateDatabase(SqlConnection connection)
     {
         using (var command = new SqlCommand(createDatabase, connection))
         {
             command.ExecuteNonQuery();
         }
     }


     private static IEnumerable<Person> CreateSamplePeople(int count)
     {
         return Enumerable.Range(0, count)
             .Select(i => new Person
                 {
                     Name = "Person" + i,
                     DateOfBirth = new DateTime(1950 + (i % 50), ((i * 3) % 12) + 1, ((i * 7) % 29) + 1)
                 });
     }
 }
}
    
