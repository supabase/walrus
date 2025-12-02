FROM supabase/postgres:17.6.1.054

# Configure PostgreSQL for logical replication
RUN echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_replication_slots = 4" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_senders = 4" >> /usr/share/postgresql/postgresql.conf.sample

# Set working directory for walrus
WORKDIR /walrus

# Copy the repository (will be mounted at runtime)
COPY . .

# Give postgres user ownership of /walrus
RUN chown -R postgres:postgres /walrus

# Create a test runner script that switches to postgres user
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Initializing PostgreSQL data directory..."\n\
su - postgres -c "initdb -D /var/lib/postgresql/data"\n\
echo "Starting PostgreSQL..."\n\
su - postgres -c "pg_ctl -D /var/lib/postgresql/data -l /var/lib/postgresql/logfile start"\n\
echo "Waiting for PostgreSQL to be ready..."\n\
until su - postgres -c "pg_isready" > /dev/null 2>&1; do\n\
  sleep 1\n\
done\n\
echo "PostgreSQL is ready!"\n\
echo "Running tests..."\n\
cd /walrus\n\
set +e\n\
su - postgres -c "cd /walrus && ./bin/installcheck"\n\
TEST_EXIT=$?\n\
set -e\n\
echo "Tests completed with exit code: $TEST_EXIT"\n\
echo ""\n\
if [ $TEST_EXIT -ne 0 ]; then\n\
  echo "========================================"\n\
  echo "TEST FAILURES DETECTED"\n\
  echo "========================================"\n\
  echo ""\n\
  if [ -f /walrus/regression.out ]; then\n\
    echo "Contents of regression.out:"\n\
    echo "========================================"\n\
    cat /walrus/regression.out\n\
    echo "========================================"\n\
  fi\n\
  echo ""\n\
  if [ -f /walrus/regression.diffs ]; then\n\
    echo "Contents of regression.diffs:"\n\
    echo "========================================"\n\
    cat /walrus/regression.diffs\n\
    echo "========================================"\n\
  fi\n\
fi\n\
su - postgres -c "pg_ctl -D /var/lib/postgresql/data stop"\n\
exit $TEST_EXIT\n\
' > /usr/local/bin/run-tests.sh && \
    chmod +x /usr/local/bin/run-tests.sh

CMD ["/usr/local/bin/run-tests.sh"]
