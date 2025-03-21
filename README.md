# connectpre
While SQLite can provide persistence, there are some limitations when used with Prefect, especially in a production environment.
Let's modify our approach to ensure proper persistence. We'll use Prefect's ability to configure a file-based SQLite database with a specific path, rather than the default in-memory database. This way, the database file persists even when Prefect is down, and Prefect can recover the state when it restarts.

1_prefect_setup.py

Then, let's ensure that we start the Prefect API server during our application startup. We'll add a function to start and stop the server:

2_prefect_server.py

Now, update our integration class to use these functions:

3_prefect_integration.py

With these changes:

1. The SQLite database will persist in a specified file location
2. The database will survive Prefect restarts
3. We explicitly start and stop the Prefect API server during application lifecycle
4. When the application restarts, Prefect will recover its state from the persistent database

This approach should address your concerns about persistence while still using SQLite as a simpler alternative to PostgreSQL.
