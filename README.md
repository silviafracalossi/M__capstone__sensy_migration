### Sensy Migration
####Database migration for "Sensy" Capstone Project
The code creates the tables on the PostgreSQL database following the new structure, loads the data from the old H2 database, transforms the format based on the changes applied and loads the data on the new PostgreSQL database.


#### Repository Structure
Inside the src/main/java folder, you can find the following files:
- H2DataExtractor, class containing all the methods for the data extraction from the old H2 database;
- Main, containing the main method, responsible of calling all the methods required for the migration;
- POSDataLoader, class containing all the methods for the tables creations and data loading to the new PosgreSQL database.

