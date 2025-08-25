# #!/bin/bash

# # Start Cassandra in the background
# # /docker-entrypoint.sh cassandra &

# # Wait until Cassandra is ready to accept connections
# echo "⏳ Waiting for Cassandra to start on port 9042..."

# RETRIES=20

# until cqlsh -e "DESCRIBE keyspaces" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
#   echo "⏳ Cassandra not ready yet... ($RETRIES)"
#   sleep 5
#   RETRIES=$((RETRIES - 1))
# done

# if [ $RETRIES -eq 0 ]; then
#   echo "❌ Cassandra did not start in time."
#   exit 1
# fi

# echo "✅ Cassandra is up. Running setup.cql..."
# cqlsh -f /setup.cql

# # Keep Cassandra running
# wait


#!/bin/bash
# Start Cassandra in the background
cassandra -R &

# Wait for Cassandra to be ready (basic check)
echo "Waiting for Cassandra to start..."
until cqlsh -e "DESCRIBE KEYSPACES"; do
  sleep 2
done

# Run your setup script
echo "Running setup.cql..."
cqlsh -f /cassandra-setup.cql

# Keep Cassandra running in the foreground
fg %1
