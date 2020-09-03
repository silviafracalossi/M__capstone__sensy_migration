## Sensy Migration

#### Database migration for "Sensy" Capstone Project
The code creates the tables on the PostgreSQL database following the new structure, loads the data from the old H2 database, transforms the format based on the changes applied and loads the data on the new PostgreSQL database.

#### Repository Structure
Inside the `src/main/java` folder, you can find the following files:
- `H2DataExtractor.java`, containing all the methods for the data extraction from the old H2 database;
- `Main.java`, containing the main method; here, you can define the addresses of the databases locations (now, my personal settings are hardcoded);
- `POSDataLoader.java`, containing all the methods for the tables creation and the data loading on the new PosgreSQL database.

#### To run the project
- H2 database - retrieve the old H2 database from the server:
    - `sudo apt install putty-tools`
    - `pscp [server_username]@sensy.inf.unibz.it:/opt/sensy/resources/-database/vincent.mv.db [path_to_this_repo]/database/server_vincent.mv.db`
- PostgreSQL database - create the credentials file to access it:
    - file name: `database/server_postgresql_credentials.txt`
    - first line: `[database_admin_username]`
    - second line: `[database_admin_password]`
- Run the project:
    - Connect to the `sensy.inf.unibz.it` server via the `Cisco AnyConnect` application
    - Open the project in Intellij IDEA
    - Compile the maven project (`mvn compile`)
    - Run the project (run main() in Main.java)
