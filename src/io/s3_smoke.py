from src.io.s3_client import list_keys, get_bytes, put_text

def main():
    prefix = "Raw/listen_events/smoke/"
    put_text(prefix + "hello.txt", "ok")
    print(list_keys(prefix))
    print(get_bytes(prefix + "hello.txt").decode())

if __name__ == "__main__":
    main()