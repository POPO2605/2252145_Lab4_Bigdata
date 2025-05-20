import socket
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lower, regexp_replace
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Cấu hình môi trường cho PySpark
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# Khai báo địa chỉ kết nối socket
SERVER_HOST = 'localhost'
SERVER_PORT = 26052004 

# Tạo SparkSession
spark = SparkSession.builder.appName("YouTubeCommentClassifier").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema cho dữ liệu nhận vào
comment_schema = StructType().add("comment", StringType())
batch_schema = ArrayType(comment_schema)

# Đọc dữ liệu stream từ socket
streaming_input = spark.readStream \
    .format("socket") \
    .option("host", SERVER_HOST) \
    .option("port", SERVER_PORT) \
    .load()

# Parse JSON từ từng dòng nhận được
structured_df = streaming_input \
    .select(from_json(col("value"), batch_schema).alias("parsed")) \
    .select(explode(col("parsed")).alias("entry")) \
    .select("entry.comment")

# Hàm huấn luyện mô hình ban đầu
def initialize_model():
    training_samples = [
        ("I love this video", 1),
        ("This is so bad", 0),
        ("Amazing content", 1),
        ("Terrible quality", 0),
        ("This is the greatest video I have ever seen", 0)
    ]
    train_df = spark.createDataFrame(training_samples, ["comment", "label"])

    tokenizer = Tokenizer(inputCol="comment", outputCol="tokens")
    stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="cleaned")
    hasher = HashingTF(inputCol="cleaned", outputCol="features", numFeatures=1000)

    text_pipeline = Pipeline(stages=[tokenizer, stopword_remover, hasher])
    prep_model = text_pipeline.fit(train_df)
    train_prepared = prep_model.transform(train_df)

    classifier = LogisticRegression(featuresCol="features", labelCol="label")
    trained_model = classifier.fit(train_prepared)

    return trained_model, text_pipeline

# Huấn luyện mô hình mẫu
classifier_model, preprocessing_pipeline = initialize_model()

# Xử lý từng batch của stream
def handle_batch(df_batch, epoch_id, model, pipeline):
    if df_batch.filter(col("comment") == "DONE").count() > 0:
        print("Đã nhận tín hiệu kết thúc")

        # Gửi lại tín hiệu xác nhận cho phía gửi
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((SERVER_HOST, SERVER_PORT))
            sock.sendall("OK200".encode())
        print("Đã gửi xác nhận kết thúc")
        query.stop()
    else:
        # Áp dụng pipeline tiền xử lý và dự đoán
        fitted_pipe = pipeline.fit(df_batch)
        transformed = fitted_pipe.transform(df_batch)
        predictions = model.transform(transformed)
        predictions.select("comment", "prediction").show(truncate=False)

# Khởi chạy stream và xử lý theo từng batch
query = structured_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(lambda df, epoch_id: handle_batch(df, epoch_id, classifier_model, preprocessing_pipeline)) \
    .start()

query.awaitTermination()
