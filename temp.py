from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("ReadFromSFTP_JSch").getOrCreate()

# Access the JVM
jvm = spark.sparkContext._gateway.jvm

# --- SFTP Connection Details ---
host = "eu-central-1.sftpcloud.io"
port = 22
username = "d17fa850640b46b0b8dca1b25ba2cdc3"
password = "EgDbdQzeUkFWnxEMvI3BcK5bWSTMDmvy"
remote_path = "/media/test/temp.txt"

# --- Step 1: Setup JSch Session ---
JSch = jvm.com.jcraft.jsch.JSch
jsch = JSch()
session = jsch.getSession(username, host, port)
session.setPassword(password)

# Disable strict host key checking
java_props = jvm.java.util.Properties()
java_props.put("StrictHostKeyChecking", "no")
session.setConfig(java_props)

print("Connecting to SFTP server...")
session.connect()
print("Connected ✅")

# --- ✅ Step 2: Open SFTP Channel (no cast) ---
channel = session.openChannel("sftp")
channel.connect()

print(f"Reading remote file: {remote_path}")

# --- Step 3: Read File into Memory ---
input_stream = channel.get(remote_path)
BufferedReader = jvm.java.io.BufferedReader
InputStreamReader = jvm.java.io.InputStreamReader
reader = BufferedReader(InputStreamReader(input_stream))

lines = []
line = reader.readLine()
while line is not None:
    lines.append(line)
    line = reader.readLine()

reader.close()
channel.disconnect()
session.disconnect()

print(f"✅ Successfully read {len(lines)} lines from SFTP file.")

# --- Step 4: Convert to Spark DataFrame ---
rdd = spark.sparkContext.parallelize(lines)
df = spark.createDataFrame(rdd.map(lambda x: (x,)), ["line"])

print("Showing data from SFTP file:")
df.show(truncate=False)

spark.stop()
