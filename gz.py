from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("ReadFromSFTP_GZ_JSch").getOrCreate()
jvm = spark.sparkContext._gateway.jvm

# --- SFTP Connection ---
host = "eu-central-1.sftpcloud.io"
port = 22
username = "d17fa850640b46b0b8dca1b25ba2cdc3"
password = "EgDbdQzeUkFWnxEMvI3BcK5bWSTMDmvy"
remote_path = "/media/test/temp.txt.gz"   # <-- GZ file path

JSch = jvm.com.jcraft.jsch.JSch
jsch = JSch()
session = jsch.getSession(username, host, port)
session.setPassword(password)
props = jvm.java.util.Properties()
props.put("StrictHostKeyChecking", "no")
session.setConfig(props)
session.connect()

channel = session.openChannel("sftp")
channel.connect()

print(f"📦 Reading remote file: {remote_path}")

# --- Handle GZ and normal text automatically ---
input_stream = channel.get(remote_path)
if remote_path.endswith(".gz"):
    GZIPInputStream = jvm.java.util.zip.GZIPInputStream
    input_stream = GZIPInputStream(input_stream)

BufferedReader = jvm.java.io.BufferedReader
InputStreamReader = jvm.java.io.InputStreamReader
reader = BufferedReader(InputStreamReader(input_stream))

# --- Read lines ---
lines = []
line = reader.readLine()
while line is not None:
    lines.append(line)
    line = reader.readLine()

reader.close()
channel.disconnect()
session.disconnect()

print(f"✅ Successfully read {len(lines)} lines from file.")

# --- Convert to DataFrame ---
header = lines[0].split(",")
data = [row.split(",") for row in lines[1:]]

schema = StructType([StructField(col, StringType(), True) for col in header])
df = spark.createDataFrame(data, schema)

print("✅ Parsed structured DataFrame:")
df.show(truncate=False)

spark.stop()
