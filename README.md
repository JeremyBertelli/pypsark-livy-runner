# Pypsark Livy Runner

A lightweight Python tool to submit and manage PySpark jobs on a remote Apache Livy server — ideal for automating Spark job execution through REST, without needing `spark-submit`.

---

## Features

- YAML-based configuration (endpoint, authentication, Spark resources, SSL)
- Session management (reuse or auto-create Livy sessions)
- Real-time output (captures and prints Livy statement logs)
- Utilities to list or clean existing Livy sessions from the CLI

---

## Get started

1. Copy `config.example.yaml` → `config.yaml` and adjust your Livy endpoint and credentials.  
2. Run a job:

   ```bash
   python main.py
   ```
This will:
- Create (or reuse) a Livy session
- Submit the script (e.g. examples/test_spark_job.py)
- Wait for completion and print output in your terminal

To run your own script, update the following line in main.py:

   ```python
script_path = "examples/my_job.py"
   python main.py
   ```


## Utilities

### Managing Sessions
List active sessions:
   ```bash
python -m pyspark_livy_runner.utils.livy_list
   ```
Delete all sessions:
   ```bash
python -m pyspark_livy_runner.utils.livy_clean
   ```

## Compatibility
Tested on:
- Cloudera OnPrem 7.1.9 (Spark 2 and Spark 3)
- Cloudera OnPrem 7.3.1 (Spark3)

## Roadmap
- Simplify job selection (parameterize which job to launch)
- Support more Knox authentication methods (token, Kerberos, etc.)

