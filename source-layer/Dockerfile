FROM postgres:17

# Set environment variables for DB

# Copy initialization script (creates tables & inserts sample data)
COPY setup.sql /docker-entrypoint-initdb.d/
