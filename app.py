import streamer
import sys

def main():
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    if len(sys.argv) < 3:
        print("Require language and hashtag.")
        print("e.g, python app.py en fsoftwareengineer")
    streamer.start_stream(sys.argv[1], sys.argv[2])


main()
