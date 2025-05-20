import socket
import time
import json
from youtube_comment_downloader import YoutubeCommentDownloader

# Khởi tạo đối tượng để tải bình luận
comment_loader = YoutubeCommentDownloader()

def retrieve_comments(video_id, max_comments=200):
    comment_stream = comment_loader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}")
    comment_list = []
    count = 0
    for entry in comment_stream:
        if count >= max_comments:
            break
        comment_list.append({"comment": entry["text"]})
        count += 1
    return comment_list

# ID của video cần lấy bình luận
youtube_video_id = "YwkOHnq_T7o"
comment_data = retrieve_comments(youtube_video_id)

# In ra bình luận cuối cùng để kiểm tra
print(comment_data[-1])

# Cấu hình địa chỉ server
HOST = 'localhost'
PORT = 26052004

# Tạo và thiết lập server socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    print(f"Server đang lắng nghe tại {HOST}:{PORT}")

    connection, client_address = server_socket.accept()
    with connection:
        print(f"Đã kết nối với {client_address}")

        # Gửi dữ liệu theo từng lô nhỏ
        chunk_size = 20
        total = len(comment_data)
        for idx in range(0, total, chunk_size):
            segment = comment_data[idx:idx + chunk_size]
            message = json.dumps(segment, ensure_ascii=False) + '\n'
            connection.sendall(message.encode())
            print(f"Đã gửi lô {(idx // chunk_size) + 1}")
            time.sleep(10)

        # Gửi tín hiệu kết thúc
        end_signal = {"comment": "DONE"}
        connection.sendall(json.dumps(end_signal, ensure_ascii=False).encode() + b'\n')
        print("Đã gửi tín hiệu kết thúc")

        # Đóng kết nối nếu nhận được xác nhận
        confirmation = connection.recv(1024).decode()
        if confirmation == "OK200":
            print("Kết nối đã được đóng đúng cách")
