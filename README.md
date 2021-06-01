**Set Up**

To run the project you need to set up "connections" in Airflow (Admin -> Connection -> Create).
1. Connection to PostgreSQL
    * Conn Id: oltp_postgres
    * Conn Type: postgres
    * Host: 127.0.0.1
    * Login: pguser
    * Password: 
    * Port: 5432
    * Extra: {"database":"dshop_bu"} - required
   
2. Connection to Greenplum
    * Conn Id: oltp_greenplum
    * Conn Type: postgres
    * Host: 127.0.0.1
    * Login: gpuser
    * Password: 
    * Port: 5433
    * Extra: {"database":"test"} - required

3. Connection to HDFS
    * Conn Id: hdfs_connection
    * Conn Type: HDFS
    * Host: 127.0.0.1
    * Login: user
    * Password: 
    * Port: 50070
    * Extra: {"bronze_stage_dir":"/bronze"} - required

4. Connection to API
    * Conn Id: out_of_stock_api
    * Conn Type: File (path)
    * Host: robot-dreams-de-api.herokuapp.com
    * Login: rd_dreams
    * Password: 
   
5. Odbc driver path
    * Conn Id: odbc_driver_path
    * Conn Type: File (path)
    * Host: /driver/path/postgresql-42.2.20.jar
