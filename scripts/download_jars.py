# scripts/download_jars.py
import urllib.request
import os

JARS_DIR = "jars"
os.makedirs(JARS_DIR, exist_ok=True)

JARS = [
    {
        "name": "delta-spark_2.12-3.0.0.jar",
        "url" : "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar"
    },
    {
        "name": "delta-storage-3.0.0.jar",
        "url" : "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar"
    },
    {
        "name": "hadoop-aws-3.3.4.jar",
        "url" : "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    },
    {
        "name": "aws-java-sdk-bundle-1.12.262.jar",
        "url" : "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    }
]

print("=" * 50)
print("Downloading Spark JAR files...")
print("=" * 50)

for jar in JARS:
    dest = os.path.join(JARS_DIR, jar["name"])
    if os.path.exists(dest):
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f"  Already exists: {jar['name']} ({size_mb:.1f} MB)")
        continue
    print(f"  Downloading: {jar['name']}...")
    urllib.request.urlretrieve(jar["url"], dest)
    size_mb = os.path.getsize(dest) / (1024 * 1024)
    print(f"  Done: {size_mb:.1f} MB")

print("\n" + "=" * 50)
print("All JARs ready in /jars folder")
print("=" * 50)
print("\nJAR files downloaded:")
for f in os.listdir(JARS_DIR):
    size = os.path.getsize(os.path.join(JARS_DIR, f)) / (1024*1024)
    print(f"  {f} ({size:.1f} MB)")